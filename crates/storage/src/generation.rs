use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const CURRENT_FILE: &str = "CURRENT";

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct GenerationManifest {
    pub version: u32,
    pub generation: String,
    pub last_applied_index: u64,
    pub files: Vec<GenerationFile>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct GenerationFile {
    pub name: String,
    pub bytes: u64,
    pub crc32c: u32,
}

#[derive(Clone)]
pub struct GenerationStore {
    pub(crate) root: PathBuf,
}

impl GenerationStore {
    pub fn open(root: impl AsRef<Path>) -> io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("generations"))?;
        Ok(Self { root })
    }

    pub fn active(&self) -> io::Result<Option<PathBuf>> {
        let name = match fs::read_to_string(self.root.join(CURRENT_FILE)) {
            Ok(value) => value.trim().to_owned(),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        validate_generation_name(&name)?;
        let directory = self.root.join("generations").join(&name);
        verify_generation(&directory, &name)?;
        Ok(Some(directory))
    }

    pub fn install(
        &self,
        generation: &str,
        last_applied_index: u64,
        files: &[(PathBuf, PathBuf)],
    ) -> io::Result<PathBuf> {
        validate_generation_name(generation)?;
        let generations = self.root.join("generations");
        let temporary = generations.join(format!(".{generation}.tmp"));
        let destination = generations.join(generation);
        if destination.exists() {
            verify_generation(&destination, generation)?;
            self.switch_current(generation)?;
            return Ok(destination);
        }
        if temporary.exists() {
            fs::remove_dir_all(&temporary)?;
        }
        fs::create_dir(&temporary)?;
        let mut manifest_files = Vec::with_capacity(files.len());
        for (source, relative) in files {
            validate_relative_path(relative)?;
            let target = temporary.join(relative);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(source, &target)?;
            File::open(&target)?.sync_all()?;
            let (bytes, crc32c) = checksum(&target)?;
            manifest_files.push(GenerationFile {
                name: relative.to_string_lossy().into_owned(),
                bytes,
                crc32c,
            });
        }
        manifest_files.sort_by(|left, right| left.name.cmp(&right.name));
        let manifest = GenerationManifest {
            version: 3,
            generation: generation.to_owned(),
            last_applied_index,
            files: manifest_files,
        };
        let manifest_path = temporary.join("manifest.json");
        let mut file = File::create(&manifest_path)?;
        file.write_all(&serde_json::to_vec_pretty(&manifest).map_err(io::Error::other)?)?;
        file.sync_all()?;
        sync_directories(&temporary)?;
        fs::rename(&temporary, &destination)?;
        File::open(&generations)?.sync_all()?;
        verify_generation(&destination, generation)?;
        self.switch_current(generation)?;
        Ok(destination)
    }

    pub fn install_bytes(
        &self,
        generation: &str,
        last_applied_index: u64,
        files: &[(PathBuf, Vec<u8>)],
    ) -> io::Result<PathBuf> {
        validate_generation_name(generation)?;
        let generations = self.root.join("generations");
        let temporary = generations.join(format!(".{generation}.tmp"));
        let destination = generations.join(generation);
        if destination.exists() {
            verify_generation(&destination, generation)?;
            self.switch_current(generation)?;
            return Ok(destination);
        }
        if temporary.exists() {
            fs::remove_dir_all(&temporary)?;
        }
        fs::create_dir(&temporary)?;
        let mut manifest_files = Vec::with_capacity(files.len());
        for (relative, contents) in files {
            validate_relative_path(relative)?;
            let target = temporary.join(relative);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut file = File::create(&target)?;
            file.write_all(contents)?;
            file.sync_all()?;
            let (bytes, crc32c) = checksum(&target)?;
            manifest_files.push(GenerationFile {
                name: relative.to_string_lossy().into_owned(),
                bytes,
                crc32c,
            });
        }
        manifest_files.sort_by(|left, right| left.name.cmp(&right.name));
        let manifest = GenerationManifest {
            version: 3,
            generation: generation.to_owned(),
            last_applied_index,
            files: manifest_files,
        };
        let manifest_path = temporary.join("manifest.json");
        let mut file = File::create(&manifest_path)?;
        file.write_all(&serde_json::to_vec_pretty(&manifest).map_err(io::Error::other)?)?;
        file.sync_all()?;
        sync_directories(&temporary)?;
        fs::rename(&temporary, &destination)?;
        File::open(&generations)?.sync_all()?;
        verify_generation(&destination, generation)?;
        self.switch_current(generation)?;
        Ok(destination)
    }

    pub(crate) fn switch_current(&self, generation: &str) -> io::Result<()> {
        let temporary = self.root.join(".CURRENT.tmp");
        let mut file = File::create(&temporary)?;
        file.write_all(generation.as_bytes())?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        fs::rename(temporary, self.root.join(CURRENT_FILE))?;
        File::open(&self.root)?.sync_all()
    }

    pub fn prune_old(&self, keep: usize) -> io::Result<usize> {
        let keep = keep.max(1);
        let current = fs::read_to_string(self.root.join(CURRENT_FILE))?
            .trim()
            .to_owned();
        validate_generation_name(&current)?;
        let generations = self.root.join("generations");
        let mut candidates = Vec::new();
        for entry in fs::read_dir(&generations)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with('.') {
                continue;
            }
            let manifest = read_generation_manifest(&entry.path(), &name)?;
            candidates.push((manifest.last_applied_index, name, entry.path()));
        }
        candidates.sort_by(|left, right| (right.0, &right.1).cmp(&(left.0, &left.1)));
        let mut retained = std::collections::BTreeSet::from([current]);
        for (_, name, _) in &candidates {
            if retained.len() >= keep {
                break;
            }
            retained.insert(name.clone());
        }
        let mut removed = 0;
        for (_, name, path) in candidates {
            if !retained.contains(&name) {
                fs::remove_dir_all(path)?;
                removed += 1;
            }
        }
        if removed > 0 {
            File::open(generations)?.sync_all()?;
        }
        Ok(removed)
    }
}

pub(crate) fn read_generation_manifest(
    directory: &Path,
    expected_name: &str,
) -> io::Result<GenerationManifest> {
    let manifest: GenerationManifest =
        serde_json::from_slice(&fs::read(directory.join("manifest.json"))?)
            .map_err(io::Error::other)?;
    if manifest.version != 3 || manifest.generation != expected_name {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid generation manifest",
        ));
    }
    Ok(manifest)
}

pub(crate) fn verify_generation(
    directory: &Path,
    expected_name: &str,
) -> io::Result<GenerationManifest> {
    let manifest = read_generation_manifest(directory, expected_name)?;
    for expected in &manifest.files {
        let relative = Path::new(&expected.name);
        validate_relative_path(relative)?;
        let (bytes, crc32c) = checksum(&directory.join(relative))?;
        if bytes != expected.bytes || crc32c != expected.crc32c {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("generation checksum mismatch for {}", expected.name),
            ));
        }
    }
    Ok(manifest)
}

pub(crate) fn checksum(path: &Path) -> io::Result<(u64, u32)> {
    use std::io::Read;
    let mut file = File::open(path)?;
    let mut checksum = 0;
    let mut bytes = 0;
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

pub(crate) fn validate_generation_name(name: &str) -> io::Result<()> {
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid generation name",
        ));
    }
    Ok(())
}

pub(crate) fn validate_relative_path(path: &Path) -> io::Result<()> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|part| !matches!(part, std::path::Component::Normal(_)))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unsafe generation file path",
        ));
    }
    Ok(())
}

pub(crate) fn sync_directories(root: &Path) -> io::Result<()> {
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
    fn installs_and_atomically_selects_verified_generation() {
        let root = tempdir().unwrap();
        let source = tempdir().unwrap();
        let file = source.path().join("state.bin");
        fs::write(&file, b"state-v2").unwrap();
        let store = GenerationStore::open(root.path()).unwrap();
        let installed = store
            .install(
                "generation-1",
                42,
                &[(file, PathBuf::from("state/state.bin"))],
            )
            .unwrap();
        assert_eq!(store.active().unwrap().unwrap(), installed);
    }

    #[test]
    fn installs_in_memory_files_with_checksums() {
        let root = tempdir().unwrap();
        let store = GenerationStore::open(root.path()).unwrap();
        let installed = store
            .install_bytes(
                "generation-2",
                84,
                &[(PathBuf::from("snapshot.json"), b"snapshot-v2".to_vec())],
            )
            .unwrap();
        assert_eq!(
            fs::read(installed.join("snapshot.json")).unwrap(),
            b"snapshot-v2"
        );
        assert_eq!(store.active().unwrap().unwrap(), installed);
    }

    #[test]
    fn retains_current_and_one_previous_generation() {
        let root = tempdir().unwrap();
        let store = GenerationStore::open(root.path()).unwrap();
        for index in 1..=3 {
            store
                .install_bytes(
                    &format!("generation-{index}"),
                    index,
                    &[(PathBuf::from("state"), vec![index as u8])],
                )
                .unwrap();
        }
        assert_eq!(store.prune_old(2).unwrap(), 1);
        assert!(store.active().unwrap().is_some());
    }
}
