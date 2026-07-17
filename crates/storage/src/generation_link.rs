use crate::generation::{
    checksum, read_generation_manifest, sync_directories, validate_generation_name,
    validate_relative_path, GenerationFile, GenerationManifest, GenerationStore,
};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};

#[derive(Clone, Debug)]
pub struct LinkedGenerationFile {
    pub source: PathBuf,
    pub file: GenerationFile,
}

impl GenerationStore {
    pub fn describe_source(
        source: impl AsRef<Path>,
        relative: impl AsRef<Path>,
    ) -> io::Result<LinkedGenerationFile> {
        validate_relative_path(relative.as_ref())?;
        let (bytes, crc32c) = checksum(source.as_ref())?;
        Ok(LinkedGenerationFile {
            source: source.as_ref().to_path_buf(),
            file: GenerationFile {
                name: relative.as_ref().to_string_lossy().into_owned(),
                bytes,
                crc32c,
            },
        })
    }

    pub fn trusted_generation_file(
        &self,
        source: impl AsRef<Path>,
        relative: impl AsRef<Path>,
    ) -> io::Result<Option<LinkedGenerationFile>> {
        validate_relative_path(relative.as_ref())?;
        let generations = self.root.join("generations");
        let Ok(path) = source.as_ref().strip_prefix(&generations) else {
            return Ok(None);
        };
        let mut components = path.components();
        let Some(Component::Normal(generation)) = components.next() else {
            return Ok(None);
        };
        let generation = generation.to_string_lossy();
        validate_generation_name(&generation)?;
        let source_relative: PathBuf = components.collect();
        validate_relative_path(&source_relative)?;
        let directory = generations.join(generation.as_ref());
        let manifest = read_generation_manifest(&directory, &generation)?;
        let source_name = source_relative.to_string_lossy();
        let Some(expected) = manifest.files.iter().find(|file| file.name == source_name) else {
            return Ok(None);
        };
        if fs::metadata(source.as_ref())?.len() != expected.bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "trusted snapshot file length changed",
            ));
        }
        Ok(Some(LinkedGenerationFile {
            source: source.as_ref().to_path_buf(),
            file: GenerationFile {
                name: relative.as_ref().to_string_lossy().into_owned(),
                bytes: expected.bytes,
                crc32c: expected.crc32c,
            },
        }))
    }

    pub fn install_linked(
        &self,
        generation: &str,
        last_applied_index: u64,
        files: &[LinkedGenerationFile],
    ) -> io::Result<PathBuf> {
        validate_generation_name(generation)?;
        let generations = self.root.join("generations");
        let temporary = generations.join(format!(".{generation}.tmp"));
        let destination = generations.join(generation);
        if destination.exists() {
            read_generation_manifest(&destination, generation)?;
            self.switch_current(generation)?;
            return Ok(destination);
        }
        if temporary.exists() {
            fs::remove_dir_all(&temporary)?;
        }
        fs::create_dir(&temporary)?;
        let mut manifest_files = Vec::with_capacity(files.len());
        for linked in files {
            let relative = Path::new(&linked.file.name);
            validate_relative_path(relative)?;
            if fs::metadata(&linked.source)?.len() != linked.file.bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "snapshot source length changed before linking",
                ));
            }
            let target = temporary.join(relative);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::hard_link(&linked.source, &target).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("immutable snapshot files must share a filesystem: {error}"),
                )
            })?;
            File::open(&target)?.sync_all()?;
            manifest_files.push(linked.file.clone());
        }
        manifest_files.sort_by(|left, right| left.name.cmp(&right.name));
        let manifest = GenerationManifest {
            version: 3,
            generation: generation.to_owned(),
            last_applied_index,
            files: manifest_files,
        };
        let mut file = File::create(temporary.join("manifest.json"))?;
        file.write_all(&serde_json::to_vec_pretty(&manifest).map_err(io::Error::other)?)?;
        file.sync_all()?;
        sync_directories(&temporary)?;
        fs::rename(&temporary, &destination)?;
        File::open(&generations)?.sync_all()?;
        self.switch_current(generation)?;
        Ok(destination)
    }

    pub fn clone_generation(
        &self,
        generation: &str,
        last_applied_index: u64,
        source: impl AsRef<Path>,
    ) -> io::Result<PathBuf> {
        let source = source.as_ref();
        let source_name = source
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid snapshot path"))?;
        let manifest = read_generation_manifest(source, source_name)?;
        if manifest.last_applied_index != last_applied_index {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot generation boundary mismatch",
            ));
        }
        let files = manifest
            .files
            .into_iter()
            .map(|file| LinkedGenerationFile {
                source: source.join(&file.name),
                file,
            })
            .collect::<Vec<_>>();
        self.install_linked(generation, last_applied_index, &files)
    }
}
