//! On-disk storage for a download in progress.
//!
//! Chunks are written at their final offset in `<dest>/.partial/<file-id>.part`,
//! so arrival order does not matter. Every chunk is verified against the
//! manifest *before* it is written. Resume needs no side-car metadata: on
//! [`ChunkStore::open`] the partial file is scanned and each chunk that already
//! hashes correctly is counted as present.

use anyhow::{bail, Context, Result};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use crate::{
    manifest::Manifest,
    util::sanitize_file_name,
};

#[derive(Debug, thiserror::Error)]
pub enum ChunkError {
    #[error("chunk index {0} is out of range")]
    OutOfRange(u32),
    #[error("chunk {index} has the wrong length (expected {expected}, got {got})")]
    WrongLength {
        index: u32,
        expected: usize,
        got: usize,
    },
    #[error("chunk {0} failed hash verification")]
    HashMismatch(u32),
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),
}

impl ChunkError {
    /// True when the data was bad (worth re-requesting); false for local
    /// failures such as a full disk.
    pub fn is_bad_data(&self) -> bool {
        !matches!(self, ChunkError::Io(_))
    }
}

pub struct ChunkStore {
    manifest: Manifest,
    part_path: PathBuf,
    dest_dir: PathBuf,
    file: File,
    have: Vec<bool>,
    have_count: u32,
}

impl ChunkStore {
    /// Open (or create) the partial file for `manifest` and discover which
    /// chunks are already present. Blocking: may read the whole partial file.
    pub fn open(dest_dir: &Path, manifest: Manifest) -> Result<Self> {
        manifest.validate()?;

        let partial_dir = dest_dir.join(".partial");
        fs::create_dir_all(&partial_dir)
            .with_context(|| format!("creating {}", partial_dir.display()))?;

        let id = manifest.id();
        let part_path = partial_dir.join(format!("{}.part", hex::encode(id)));

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&part_path)
            .with_context(|| format!("opening {}", part_path.display()))?;

        let len = file.metadata()?.len();
        if len > manifest.size {
            file.set_len(manifest.size)?;
        }
        let len = len.min(manifest.size);

        let count = manifest.chunk_count();
        let mut have = vec![false; count as usize];
        let mut have_count = 0u32;
        let mut buf = vec![0u8; manifest.chunk_size as usize];

        for index in 0..count {
            let offset = manifest.chunk_offset(index);
            let chunk_len = manifest.chunk_len(index).expect("index in range");
            if offset + chunk_len as u64 > len {
                break; // offsets only grow, nothing later can be present
            }
            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(&mut buf[..chunk_len])?;
            if blake3::hash(&buf[..chunk_len]).as_bytes() == &manifest.chunk_hashes[index as usize]
            {
                have[index as usize] = true;
                have_count += 1;
            }
        }

        Ok(ChunkStore {
            manifest,
            part_path,
            dest_dir: dest_dir.to_path_buf(),
            file,
            have,
            have_count,
        })
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    pub fn have_count(&self) -> u32 {
        self.have_count
    }

    pub fn is_complete(&self) -> bool {
        self.have_count == self.manifest.chunk_count()
    }

    /// Indices of chunks we still need, in ascending order.
    pub fn missing(&self) -> Vec<u32> {
        self.have
            .iter()
            .enumerate()
            .filter(|(_, present)| !**present)
            .map(|(i, _)| i as u32)
            .collect()
    }

    /// Bytes already present on disk (verified).
    pub fn bytes_have(&self) -> u64 {
        self.have
            .iter()
            .enumerate()
            .filter(|(_, present)| **present)
            .map(|(i, _)| self.manifest.chunk_len(i as u32).unwrap_or(0) as u64)
            .sum()
    }

    /// Verify and store one chunk. Returns `Ok(true)` if it was new,
    /// `Ok(false)` if we already had it.
    pub fn write_chunk(&mut self, index: u32, data: &[u8]) -> Result<bool, ChunkError> {
        let expected = self
            .manifest
            .chunk_len(index)
            .ok_or(ChunkError::OutOfRange(index))?;
        if data.len() != expected {
            return Err(ChunkError::WrongLength {
                index,
                expected,
                got: data.len(),
            });
        }
        if blake3::hash(data).as_bytes() != &self.manifest.chunk_hashes[index as usize] {
            return Err(ChunkError::HashMismatch(index));
        }
        if self.have[index as usize] {
            return Ok(false);
        }

        self.file
            .seek(SeekFrom::Start(self.manifest.chunk_offset(index)))?;
        self.file.write_all(data)?;
        self.have[index as usize] = true;
        self.have_count += 1;
        Ok(true)
    }

    /// Move the finished file into place. Requires every chunk to be present.
    /// Never overwrites an existing file: collisions become `name (1).ext`.
    pub fn finalize(mut self) -> Result<PathBuf> {
        if !self.is_complete() {
            bail!(
                "download incomplete: {}/{} chunks",
                self.have_count,
                self.manifest.chunk_count()
            );
        }

        self.file.flush()?;
        self.file.sync_all()?;
        let len = self.file.metadata()?.len();
        if len != self.manifest.size {
            bail!(
                "size mismatch after download: expected {}, found {}",
                self.manifest.size,
                len
            );
        }

        let name = sanitize_file_name(&self.manifest.name);
        let final_path = reserve_unique_path(&self.dest_dir, &name)?;

        let part_path = self.part_path.clone();
        drop(self.file);
        fs::rename(&part_path, &final_path)
            .with_context(|| format!("moving file to {}", final_path.display()))?;
        Ok(final_path)
    }
}

/// Atomically claim a not-yet-existing path in `dir`, trying `name`,
/// `stem (1).ext`, `stem (2).ext`, ...
fn reserve_unique_path(dir: &Path, name: &str) -> io::Result<PathBuf> {
    let (stem, ext) = match name.rfind('.') {
        Some(pos) if pos > 0 => (&name[..pos], &name[pos..]),
        _ => (name, ""),
    };

    let mut n = 0u32;
    loop {
        let candidate = if n == 0 {
            name.to_string()
        } else {
            format!("{} ({}){}", stem, n, ext)
        };
        let path = dir.join(candidate);
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(_) => return Ok(path),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => n += 1,
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a manifest for in-memory data without touching disk.
    fn manifest_for(name: &str, data: &[u8], chunk_size: u32) -> Manifest {
        let chunk_hashes = data
            .chunks(chunk_size as usize)
            .map(|c| *blake3::hash(c).as_bytes())
            .collect();
        Manifest {
            version: crate::manifest::MANIFEST_VERSION,
            name: name.to_string(),
            size: data.len() as u64,
            chunk_size,
            chunk_hashes,
        }
    }

    fn sample(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i * 31 % 256) as u8).collect()
    }

    #[test]
    fn full_download_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let data = sample(10_000);
        let m = manifest_for("file.bin", &data, 1024);
        let mut store = ChunkStore::open(dir.path(), m.clone()).unwrap();
        assert_eq!(store.missing().len(), 10);

        for (i, chunk) in data.chunks(1024).enumerate() {
            assert!(store.write_chunk(i as u32, chunk).unwrap());
        }
        assert!(store.is_complete());
        let out = store.finalize().unwrap();
        assert_eq!(fs::read(out).unwrap(), data);
    }

    #[test]
    fn rejects_corrupt_wrong_length_and_out_of_range() {
        let dir = tempfile::tempdir().unwrap();
        let data = sample(3000);
        let mut store = ChunkStore::open(dir.path(), manifest_for("f", &data, 1024)).unwrap();

        let mut bad = data[..1024].to_vec();
        bad[0] ^= 0xff;
        assert!(matches!(
            store.write_chunk(0, &bad),
            Err(ChunkError::HashMismatch(0))
        ));
        assert!(matches!(
            store.write_chunk(0, &data[..100]),
            Err(ChunkError::WrongLength { .. })
        ));
        assert!(matches!(
            store.write_chunk(99, &data[..1024]),
            Err(ChunkError::OutOfRange(99))
        ));
        assert_eq!(store.have_count(), 0);
        assert!(ChunkError::HashMismatch(0).is_bad_data());
    }

    #[test]
    fn accepts_chunks_out_of_order_and_ignores_duplicates() {
        let dir = tempfile::tempdir().unwrap();
        let data = sample(4096);
        let mut store = ChunkStore::open(dir.path(), manifest_for("o", &data, 1024)).unwrap();
        let chunks: Vec<&[u8]> = data.chunks(1024).collect();

        for i in [3usize, 1, 0, 2] {
            assert!(store.write_chunk(i as u32, chunks[i]).unwrap());
        }
        assert!(!store.write_chunk(2, chunks[2]).unwrap()); // duplicate
        let out = store.finalize().unwrap();
        assert_eq!(fs::read(out).unwrap(), data);
    }

    #[test]
    fn resume_picks_up_verified_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let data = sample(5000);
        let m = manifest_for("resume.bin", &data, 1024);
        let chunks: Vec<&[u8]> = data.chunks(1024).collect();

        {
            let mut store = ChunkStore::open(dir.path(), m.clone()).unwrap();
            store.write_chunk(0, chunks[0]).unwrap();
            store.write_chunk(1, chunks[1]).unwrap();
            // dropped: simulates a crash / disconnect
        }

        let mut store = ChunkStore::open(dir.path(), m).unwrap();
        assert_eq!(store.have_count(), 2);
        assert_eq!(store.missing(), vec![2, 3, 4]);
        assert_eq!(store.bytes_have(), 2048);

        for i in 2..5usize {
            store.write_chunk(i as u32, chunks[i]).unwrap();
        }
        let out = store.finalize().unwrap();
        assert_eq!(fs::read(out).unwrap(), data);
    }

    #[test]
    fn resume_discards_corrupted_partial_data() {
        let dir = tempfile::tempdir().unwrap();
        let data = sample(3000);
        let m = manifest_for("c.bin", &data, 1024);
        let chunks: Vec<&[u8]> = data.chunks(1024).collect();

        let part_path;
        {
            let mut store = ChunkStore::open(dir.path(), m.clone()).unwrap();
            store.write_chunk(0, chunks[0]).unwrap();
            store.write_chunk(1, chunks[1]).unwrap();
            part_path = store.part_path.clone();
        }
        // Corrupt a byte inside chunk 1 on disk.
        let mut raw = fs::read(&part_path).unwrap();
        raw[1500] ^= 0xff;
        fs::write(&part_path, raw).unwrap();

        let store = ChunkStore::open(dir.path(), m).unwrap();
        assert_eq!(store.have_count(), 1);
        assert_eq!(store.missing(), vec![1, 2]);
    }

    #[test]
    fn finalize_requires_completeness() {
        let dir = tempfile::tempdir().unwrap();
        let data = sample(2048);
        let store = ChunkStore::open(dir.path(), manifest_for("i", &data, 1024)).unwrap();
        assert!(store.finalize().is_err());
    }

    #[test]
    fn finalize_never_overwrites() {
        let dir = tempfile::tempdir().unwrap();
        let data = sample(1500);
        let m = manifest_for("same.txt", &data, 1024);

        let mut paths = Vec::new();
        for _ in 0..3 {
            let mut store = ChunkStore::open(dir.path(), m.clone()).unwrap();
            for (i, c) in data.chunks(1024).enumerate() {
                store.write_chunk(i as u32, c).unwrap();
            }
            paths.push(store.finalize().unwrap());
        }
        let names: Vec<String> = paths
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
            .collect();
        assert_eq!(names, vec!["same.txt", "same (1).txt", "same (2).txt"]);
    }

    #[test]
    fn malicious_name_stays_inside_dest_dir() {
        let dir = tempfile::tempdir().unwrap();
        let data = sample(10);
        let m = manifest_for("../../evil.sh", &data, 1024);
        let mut store = ChunkStore::open(dir.path(), m).unwrap();
        store.write_chunk(0, &data).unwrap();
        let out = store.finalize().unwrap();
        assert_eq!(out.parent().unwrap(), dir.path());
        assert_eq!(out.file_name().unwrap(), "evil.sh");
    }

    #[test]
    fn empty_file_finalizes() {
        let dir = tempfile::tempdir().unwrap();
        let m = manifest_for("empty", &[], 1024);
        let store = ChunkStore::open(dir.path(), m).unwrap();
        assert!(store.is_complete());
        let out = store.finalize().unwrap();
        assert_eq!(fs::metadata(out).unwrap().len(), 0);
    }
}
