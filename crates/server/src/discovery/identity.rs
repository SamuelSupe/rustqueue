use anyhow::Context;
use libp2p::identity::Keypair;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

pub fn load_or_create(path: &Path) -> anyhow::Result<Keypair> {
    match fs::read(path) {
        Ok(bytes) => Keypair::from_protobuf_encoding(&bytes)
            .with_context(|| format!("decode discovery identity {}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => create(path),
        Err(error) => Err(error).with_context(|| format!("read identity {}", path.display())),
    }
}

fn create(path: &Path) -> anyhow::Result<Keypair> {
    let parent = path
        .parent()
        .context("discovery identity path has no parent")?;
    fs::create_dir_all(parent)?;
    let key = Keypair::generate_ed25519();
    let encoded = key
        .to_protobuf_encoding()
        .context("encode discovery identity")?;
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("identity"),
        std::process::id()
    ));
    let _ = fs::remove_file(&temporary);
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&temporary)
        .with_context(|| format!("create identity {}", temporary.display()))?;
    file.write_all(&encoded)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    sync_directory(parent)?;
    Ok(key)
}

fn sync_directory(path: &Path) -> anyhow::Result<()> {
    let directory = OpenOptions::new().read(true).open(path)?;
    directory.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_is_stable_across_reloads() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("identity.key");
        let first = load_or_create(&path).unwrap().public().to_peer_id();
        let second = load_or_create(&path).unwrap().public().to_peer_id();
        assert_eq!(first, second);
    }
}
