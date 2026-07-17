use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SnapshotFile {
    pub name: String,
    pub bytes: u64,
    pub crc32c: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SnapshotManifest {
    pub version: u32,
    pub last_applied_index: u64,
    pub files: Vec<SnapshotFile>,
}

pub struct SnapshotStore {
    root: PathBuf,
}

impl SnapshotStore {
    pub fn new(root: impl AsRef<Path>) -> io::Result<Self> {
        fs::create_dir_all(root.as_ref())?;
        Ok(Self {
            root: root.as_ref().to_path_buf(),
        })
    }

    pub fn export(
        &self,
        name: &str,
        last_applied_index: u64,
        source_files: &[PathBuf],
    ) -> io::Result<PathBuf> {
        validate_snapshot_name(name)?;
        let files: Vec<_> = source_files
            .iter()
            .map(|source| {
                let name = source
                    .file_name()
                    .and_then(|value| value.to_str())
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "invalid snapshot file")
                    })?;
                Ok((PathBuf::from(name), source.clone()))
            })
            .collect::<io::Result<_>>()?;
        self.export_named(name, last_applied_index, &files)
    }

    pub fn export_tree(
        &self,
        name: &str,
        last_applied_index: u64,
        source_root: &Path,
        source_files: &[PathBuf],
    ) -> io::Result<PathBuf> {
        let files = source_files
            .iter()
            .map(|source| {
                let relative = source.strip_prefix(source_root).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "snapshot source is outside source root",
                    )
                })?;
                validate_relative_path(relative)?;
                Ok((relative.to_path_buf(), source.clone()))
            })
            .collect::<io::Result<Vec<_>>>()?;
        self.export_named(name, last_applied_index, &files)
    }

    fn export_named(
        &self,
        name: &str,
        last_applied_index: u64,
        source_files: &[(PathBuf, PathBuf)],
    ) -> io::Result<PathBuf> {
        validate_snapshot_name(name)?;
        let temporary = self.root.join(format!(".{name}.tmp"));
        let destination = self.root.join(name);
        if temporary.exists() {
            fs::remove_dir_all(&temporary)?;
        }
        fs::create_dir(&temporary)?;

        let mut files = Vec::with_capacity(source_files.len());
        for (relative, source) in source_files {
            validate_relative_path(relative)?;
            let target = temporary.join(relative);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(source, &target)?;
            let (bytes, crc32c) = checksum(&target)?;
            files.push(SnapshotFile {
                name: relative.to_string_lossy().into_owned(),
                bytes,
                crc32c,
            });
        }
        let manifest = SnapshotManifest {
            version: 3,
            last_applied_index,
            files,
        };
        let bytes = serde_json::to_vec_pretty(&manifest).map_err(io::Error::other)?;
        let mut manifest_file = File::create(temporary.join("manifest.json"))?;
        manifest_file.write_all(&bytes)?;
        manifest_file.sync_all()?;
        File::open(&temporary)?.sync_all()?;
        if destination.exists() {
            fs::remove_dir_all(&destination)?;
        }
        fs::rename(&temporary, &destination)?;
        File::open(&self.root)?.sync_all()?;
        Ok(destination)
    }

    pub fn verify(&self, name: &str) -> io::Result<SnapshotManifest> {
        validate_snapshot_name(name)?;
        let directory = self.root.join(name);
        let manifest: SnapshotManifest =
            serde_json::from_slice(&fs::read(directory.join("manifest.json"))?)
                .map_err(io::Error::other)?;
        if manifest.version != 3 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported snapshot version",
            ));
        }
        for expected in &manifest.files {
            let relative = Path::new(&expected.name);
            validate_relative_path(relative)?;
            let path = directory.join(relative);
            let (bytes, crc32c) = checksum(&path)?;
            if bytes != expected.bytes || crc32c != expected.crc32c {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("snapshot checksum mismatch for {}", expected.name),
                ));
            }
        }
        Ok(manifest)
    }

    pub fn restore(&self, name: &str, target: &Path) -> io::Result<SnapshotManifest> {
        let manifest = self.verify(name)?;
        if target.exists() && fs::read_dir(target)?.next().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "restore target must be empty or absent",
            ));
        }
        let parent = target.parent().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "restore target has no parent")
        })?;
        fs::create_dir_all(parent)?;
        let temporary = parent.join(format!(".rustqueue-restore-{name}.tmp"));
        if temporary.exists() {
            fs::remove_dir_all(&temporary)?;
        }
        fs::create_dir(&temporary)?;
        let source = self.root.join(name);
        for file in &manifest.files {
            let relative = Path::new(&file.name);
            validate_relative_path(relative)?;
            let destination = temporary.join(relative);
            if let Some(parent) = destination.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(source.join(relative), &destination)?;
            File::open(&destination)?.sync_all()?;
        }
        sync_tree_directories(&temporary)?;
        if target.exists() {
            fs::remove_dir(target)?;
        }
        fs::rename(&temporary, target)?;
        File::open(parent)?.sync_all()?;
        Ok(manifest)
    }
}

fn checksum(path: &Path) -> io::Result<(u64, u32)> {
    let mut file = File::open(path)?;
    let mut checksum = 0;
    let mut bytes = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        checksum = crc32c::crc32c_append(checksum, &buffer[..read]);
        bytes += read as u64;
    }
    Ok((bytes, checksum))
}

fn validate_snapshot_name(name: &str) -> io::Result<()> {
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "snapshot name contains unsupported characters",
        ));
    }
    Ok(())
}

fn validate_relative_path(path: &Path) -> io::Result<()> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "snapshot path is unsafe",
        ));
    }
    Ok(())
}

fn sync_tree_directories(root: &Path) -> io::Result<()> {
    let mut directories = vec![root.to_path_buf()];
    let mut cursor = 0;
    while cursor < directories.len() {
        let directory = directories[cursor].clone();
        cursor += 1;
        for entry in fs::read_dir(&directory)? {
            let path = entry?.path();
            if path.is_dir() {
                directories.push(path);
            }
        }
    }
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        File::open(directory)?.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn exports_and_verifies_snapshot() {
        let source = tempdir().unwrap();
        let snapshots = tempdir().unwrap();
        let segment = source.path().join("segment.rqlog");
        fs::write(&segment, b"durable data").unwrap();
        let store = SnapshotStore::new(snapshots.path()).unwrap();
        store.export("snap-1", 42, &[segment]).unwrap();
        assert_eq!(store.verify("snap-1").unwrap().last_applied_index, 42);
    }

    #[test]
    fn preserves_tree_and_restores_to_empty_target() {
        let source = tempdir().unwrap();
        let snapshots = tempdir().unwrap();
        let restore_parent = tempdir().unwrap();
        fs::create_dir_all(source.path().join("topics/a")).unwrap();
        let segment = source.path().join("topics/a/segment.rqlog");
        fs::write(&segment, b"tree data").unwrap();
        let store = SnapshotStore::new(snapshots.path()).unwrap();
        store
            .export_tree("tree", 7, source.path(), &[segment])
            .unwrap();
        let target = restore_parent.path().join("restored");
        store.restore("tree", &target).unwrap();
        assert_eq!(
            fs::read(target.join("topics/a/segment.rqlog")).unwrap(),
            b"tree data"
        );
    }
}
