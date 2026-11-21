//! The manifest describes a file without containing it: name, size, chunk size
//! and the BLAKE3 hash of every chunk. The BLAKE3 hash of the canonical
//! serialised manifest is the file's identity ([`FileId`]).
//!
//! Two peers holding the same `FileId` hold the same bytes. That property is
//! what later makes multi-peer (swarm) download possible.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::Read,
    path::Path,
};

use crate::util::MAX_FILE_NAME_BYTES;

pub type Hash = [u8; 32];
pub type FileId = Hash;

pub const MANIFEST_VERSION: u8 = 1;
pub const DEFAULT_CHUNK_SIZE: u32 = 1024 * 1024; // 1 MiB
pub const MAX_CHUNK_SIZE: u32 = 4 * 1024 * 1024; // 4 MiB

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub version: u8,
    /// Display name only. Never used as a path without sanitising.
    pub name: String,
    pub size: u64,
    pub chunk_size: u32,
    /// BLAKE3 hash of each chunk, in order.
    pub chunk_hashes: Vec<Hash>,
}

impl Manifest {
    /// Hash a file on disk into a manifest. Blocking: run it on a blocking
    /// thread (`spawn_blocking`) when called from async code.
    pub fn from_file(path: &Path, chunk_size: u32) -> Result<Self> {
        if chunk_size == 0 || chunk_size > MAX_CHUNK_SIZE {
            bail!("invalid chunk size {}", chunk_size);
        }

        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .context("file has no valid UTF-8 name")?
            .to_string();

        let mut file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
        let size = file.metadata()?.len();

        let mut chunk_hashes = Vec::with_capacity(expected_chunk_count(size, chunk_size));
        let mut buf = vec![0u8; chunk_size as usize];
        let mut remaining = size;

        while remaining > 0 {
            let want = remaining.min(chunk_size as u64) as usize;
            file.read_exact(&mut buf[..want])
                .context("file changed size while hashing")?;
            chunk_hashes.push(*blake3::hash(&buf[..want]).as_bytes());
            remaining -= want as u64;
        }

        Ok(Manifest {
            version: MANIFEST_VERSION,
            name,
            size,
            chunk_size,
            chunk_hashes,
        })
    }

    /// The file's identity: BLAKE3 of the canonical (postcard) encoding.
    pub fn id(&self) -> FileId {
        let bytes = postcard::to_allocvec(self).expect("manifest serialisation cannot fail");
        *blake3::hash(&bytes).as_bytes()
    }

    pub fn chunk_count(&self) -> u32 {
        self.chunk_hashes.len() as u32
    }

    /// Length in bytes of chunk `index` (the last chunk may be shorter).
    pub fn chunk_len(&self, index: u32) -> Option<usize> {
        let count = self.chunk_count();
        if index >= count {
            return None;
        }
        if index + 1 == count {
            Some((self.size - index as u64 * self.chunk_size as u64) as usize)
        } else {
            Some(self.chunk_size as usize)
        }
    }

    /// Byte offset of chunk `index` in the file.
    pub fn chunk_offset(&self, index: u32) -> u64 {
        index as u64 * self.chunk_size as u64
    }

    /// Check a manifest received from the network before trusting any of it.
    pub fn validate(&self) -> Result<()> {
        if self.version != MANIFEST_VERSION {
            bail!("unsupported manifest version {}", self.version);
        }
        if self.name.is_empty() || self.name.len() > MAX_FILE_NAME_BYTES {
            bail!("invalid file name length");
        }
        if self.chunk_size == 0 || self.chunk_size > MAX_CHUNK_SIZE {
            bail!("invalid chunk size {}", self.chunk_size);
        }
        let expected = expected_chunk_count(self.size, self.chunk_size);
        if self.chunk_hashes.len() != expected {
            bail!(
                "chunk count mismatch: manifest has {}, size implies {}",
                self.chunk_hashes.len(),
                expected
            );
        }
        Ok(())
    }
}

pub fn expected_chunk_count(size: u64, chunk_size: u32) -> usize {
    if size == 0 {
        0
    } else {
        ((size - 1) / chunk_size as u64 + 1) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_file(dir: &Path, name: &str, data: &[u8]) -> std::path::PathBuf {
        let p = dir.join(name);
        File::create(&p).unwrap().write_all(data).unwrap();
        p
    }

    #[test]
    fn same_content_same_id() {
        let dir = tempfile::tempdir().unwrap();
        let data: Vec<u8> = (0..10_000u32).map(|i| (i % 251) as u8).collect();
        let a = write_file(dir.path(), "a.bin", &data);
        let m1 = Manifest::from_file(&a, 1024).unwrap();
        let m2 = Manifest::from_file(&a, 1024).unwrap();
        assert_eq!(m1.id(), m2.id());
        assert_eq!(m1.chunk_count(), 10); // 10_000 / 1024 -> 10 chunks
        assert_eq!(m1.chunk_len(9), Some(10_000 - 9 * 1024));
        assert_eq!(m1.chunk_len(10), None);
    }

    #[test]
    fn flipped_byte_changes_id_and_hash() {
        let dir = tempfile::tempdir().unwrap();
        let mut data: Vec<u8> = vec![7u8; 5000];
        let a = write_file(dir.path(), "x.bin", &data);
        let m1 = Manifest::from_file(&a, 1024).unwrap();

        data[2500] ^= 1; // lives in chunk 2
        let b = write_file(dir.path(), "x.bin", &data);
        let m2 = Manifest::from_file(&b, 1024).unwrap();

        assert_ne!(m1.id(), m2.id());
        assert_eq!(m1.chunk_hashes[0], m2.chunk_hashes[0]);
        assert_ne!(m1.chunk_hashes[2], m2.chunk_hashes[2]);
    }

    #[test]
    fn name_participates_in_identity() {
        let dir = tempfile::tempdir().unwrap();
        let a = write_file(dir.path(), "a.txt", b"hello");
        let b = write_file(dir.path(), "b.txt", b"hello");
        let ma = Manifest::from_file(&a, 1024).unwrap();
        let mb = Manifest::from_file(&b, 1024).unwrap();
        assert_ne!(ma.id(), mb.id());
        assert_eq!(ma.chunk_hashes, mb.chunk_hashes);
    }

    #[test]
    fn empty_file_has_no_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let a = write_file(dir.path(), "empty", b"");
        let m = Manifest::from_file(&a, 1024).unwrap();
        assert_eq!(m.chunk_count(), 0);
        m.validate().unwrap();
    }

    #[test]
    fn exact_multiple_of_chunk_size() {
        let dir = tempfile::tempdir().unwrap();
        let a = write_file(dir.path(), "exact", &vec![1u8; 2048]);
        let m = Manifest::from_file(&a, 1024).unwrap();
        assert_eq!(m.chunk_count(), 2);
        assert_eq!(m.chunk_len(1), Some(1024));
    }

    #[test]
    fn validate_rejects_bad_manifests() {
        let dir = tempfile::tempdir().unwrap();
        let a = write_file(dir.path(), "v", &vec![1u8; 3000]);
        let good = Manifest::from_file(&a, 1024).unwrap();
        good.validate().unwrap();

        let mut m = good.clone();
        m.chunk_hashes.pop();
        assert!(m.validate().is_err());

        let mut m = good.clone();
        m.chunk_size = 0;
        assert!(m.validate().is_err());

        let mut m = good.clone();
        m.version = 99;
        assert!(m.validate().is_err());

        let mut m = good.clone();
        m.name = String::new();
        assert!(m.validate().is_err());

        // A lie about size must not match the chunk count.
        let mut m = good;
        m.size *= 1000;
        assert!(m.validate().is_err());
    }

    #[test]
    fn roundtrips_through_postcard() {
        let dir = tempfile::tempdir().unwrap();
        let a = write_file(dir.path(), "rt", &vec![9u8; 4000]);
        let m = Manifest::from_file(&a, 1024).unwrap();
        let bytes = postcard::to_allocvec(&m).unwrap();
        let back: Manifest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(m, back);
        assert_eq!(m.id(), back.id());
    }
}
