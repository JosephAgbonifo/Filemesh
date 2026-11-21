//! Persistent peer identity, so your PeerId survives restarts.

use anyhow::{Context, Result};
use libp2p::identity::Keypair;
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use crate::util::sanitize_file_name;

/// `~/.filemesh/<name>.key` (falls back to the current directory if no home
/// directory can be found).
pub fn default_key_path(peer_name: &str) -> PathBuf {
    let base = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    base.join(".filemesh")
        .join(format!("{}.key", sanitize_file_name(peer_name)))
}

/// Load the keypair at `path`, or generate and save a new one.
pub fn load_or_create(path: &Path) -> Result<Keypair> {
    if path.exists() {
        let bytes = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        return Keypair::from_protobuf_encoding(&bytes)
            .with_context(|| format!("{} is not a valid key file", path.display()));
    }

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    }

    let keypair = Keypair::generate_ed25519();
    let bytes = keypair
        .to_protobuf_encoding()
        .context("encoding keypair")?;

    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600); // private key: owner read/write only
    }
    let mut file = options
        .open(path)
        .with_context(|| format!("creating {}", path.display()))?;
    file.write_all(&bytes)?;

    Ok(keypair)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_persists_across_loads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sub").join("alice.key");

        let first = load_or_create(&path).unwrap();
        let second = load_or_create(&path).unwrap();
        assert_eq!(first.public().to_peer_id(), second.public().to_peer_id());
    }

    #[test]
    fn different_files_give_different_identities() {
        let dir = tempfile::tempdir().unwrap();
        let a = load_or_create(&dir.path().join("a.key")).unwrap();
        let b = load_or_create(&dir.path().join("b.key")).unwrap();
        assert_ne!(a.public().to_peer_id(), b.public().to_peer_id());
    }

    #[test]
    fn corrupt_key_file_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.key");
        fs::write(&path, b"definitely not a key").unwrap();
        assert!(load_or_create(&path).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn key_file_is_private() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("p.key");
        load_or_create(&path).unwrap();
        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }
}
