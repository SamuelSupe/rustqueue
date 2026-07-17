use crate::generation::{
    sync_directories, validate_generation_name, validate_relative_path, GenerationFile,
    GenerationManifest, GenerationStore,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"RQSARCH1";
const HEADER_BYTES: usize = 16;
const MAX_MANIFEST_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct SnapshotArchivePlan {
    pub header: Vec<u8>,
    pub files: Vec<(PathBuf, u64)>,
    pub total_bytes: u64,
}

#[derive(Deserialize, Serialize)]
struct ArchiveManifest {
    version: u32,
    last_applied_index: u64,
    files: Vec<GenerationFile>,
}

pub fn snapshot_archive_plan(directory: impl AsRef<Path>) -> io::Result<SnapshotArchivePlan> {
    let directory = directory.as_ref();
    let generation = directory
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid generation path"))?;
    validate_generation_name(generation)?;
    let manifest: GenerationManifest =
        serde_json::from_slice(&fs::read(directory.join("manifest.json"))?)
            .map_err(io::Error::other)?;
    if manifest.version != 3 || manifest.generation != generation {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot generation manifest mismatch",
        ));
    }
    let archive = ArchiveManifest {
        version: 1,
        last_applied_index: manifest.last_applied_index,
        files: manifest.files.clone(),
    };
    let encoded = serde_json::to_vec(&archive).map_err(io::Error::other)?;
    if encoded.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot archive manifest is too large",
        ));
    }
    let mut header = Vec::with_capacity(HEADER_BYTES + encoded.len());
    header.extend_from_slice(MAGIC);
    header.extend_from_slice(&(encoded.len() as u64).to_be_bytes());
    header.extend_from_slice(&encoded);
    let mut total_bytes = header.len() as u64;
    let mut files = Vec::with_capacity(manifest.files.len());
    for expected in &manifest.files {
        let relative = Path::new(&expected.name);
        validate_relative_path(relative)?;
        let path = directory.join(relative);
        if fs::metadata(&path)?.len() != expected.bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("snapshot file length changed for {}", expected.name),
            ));
        }
        total_bytes = total_bytes
            .checked_add(expected.bytes)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "snapshot size overflow"))?;
        files.push((path, expected.bytes));
    }
    Ok(SnapshotArchivePlan {
        header,
        files,
        total_bytes,
    })
}

impl GenerationStore {
    pub fn install_archive(
        &self,
        generation: &str,
        expected_last_applied: u64,
        archive_path: impl AsRef<Path>,
    ) -> io::Result<PathBuf> {
        validate_generation_name(generation)?;
        let generations = self.root.join("generations");
        let temporary = generations.join(format!(".{generation}.tmp"));
        let destination = generations.join(generation);
        if destination.exists() {
            super::generation::verify_generation(&destination, generation)?;
            self.switch_current(generation)?;
            return Ok(destination);
        }
        if temporary.exists() {
            fs::remove_dir_all(&temporary)?;
        }
        let mut source = File::open(archive_path)?;
        let mut header = [0u8; HEADER_BYTES];
        source.read_exact(&mut header)?;
        if &header[..8] != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid snapshot archive magic",
            ));
        }
        let manifest_bytes = u64::from_be_bytes(header[8..16].try_into().unwrap());
        if manifest_bytes > MAX_MANIFEST_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot archive manifest is too large",
            ));
        }
        let mut encoded = vec![0; manifest_bytes as usize];
        source.read_exact(&mut encoded)?;
        let archive: ArchiveManifest =
            serde_json::from_slice(&encoded).map_err(io::Error::other)?;
        if archive.version != 1 || archive.last_applied_index != expected_last_applied {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot archive boundary mismatch",
            ));
        }
        let mut names = BTreeSet::new();
        for file in &archive.files {
            validate_relative_path(Path::new(&file.name))?;
            if !names.insert(file.name.clone()) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "snapshot archive contains duplicate files",
                ));
            }
        }
        fs::create_dir(&temporary)?;
        let result = extract_files(&mut source, &temporary, &archive.files).and_then(|_| {
            if source.stream_position()? != source.metadata()?.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "snapshot archive has trailing bytes",
                ));
            }
            let manifest = GenerationManifest {
                version: 3,
                generation: generation.to_owned(),
                last_applied_index: expected_last_applied,
                files: archive.files,
            };
            let mut file = File::create(temporary.join("manifest.json"))?;
            file.write_all(&serde_json::to_vec_pretty(&manifest).map_err(io::Error::other)?)?;
            file.sync_all()?;
            sync_directories(&temporary)?;
            fs::rename(&temporary, &destination)?;
            File::open(&generations)?.sync_all()?;
            super::generation::verify_generation(&destination, generation)?;
            self.switch_current(generation)?;
            Ok(destination.clone())
        });
        if result.is_err() && temporary.exists() {
            let _ = fs::remove_dir_all(&temporary);
        }
        result
    }
}

fn extract_files(source: &mut File, root: &Path, files: &[GenerationFile]) -> io::Result<()> {
    let mut buffer = vec![0u8; 1024 * 1024];
    for expected in files {
        let path = root.join(&expected.name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut target = File::create(&path)?;
        let mut remaining = expected.bytes;
        let mut checksum = 0u32;
        while remaining > 0 {
            let wanted = remaining.min(buffer.len() as u64) as usize;
            source.read_exact(&mut buffer[..wanted])?;
            target.write_all(&buffer[..wanted])?;
            checksum = crc32c::crc32c_append(checksum, &buffer[..wanted]);
            remaining -= wanted as u64;
        }
        if checksum != expected.crc32c {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("snapshot checksum mismatch for {}", expected.name),
            ));
        }
        target.sync_all()?;
    }
    Ok(())
}
