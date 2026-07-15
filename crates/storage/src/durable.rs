use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct DurableMetadata {
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub committed_index: u64,
    pub applied_index: u64,
}

pub struct MetadataStore {
    path: PathBuf,
}

impl MetadataStore {
    pub fn new(directory: impl AsRef<Path>) -> io::Result<Self> {
        fs::create_dir_all(directory.as_ref())?;
        Ok(Self {
            path: directory.as_ref().join("hard-state.json"),
        })
    }

    pub fn load(&self) -> io::Result<DurableMetadata> {
        match fs::read(&self.path) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(io::Error::other),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(DurableMetadata::default()),
            Err(error) => Err(error),
        }
    }

    pub fn store(&self, metadata: &DurableMetadata) -> io::Result<()> {
        let parent = self.path.parent().expect("metadata path has a parent");
        let temporary = parent.join("hard-state.json.tmp");
        let bytes = serde_json::to_vec(metadata).map_err(io::Error::other)?;
        let mut file = File::create(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, &self.path)?;
        File::open(parent)?.sync_all()
    }
}
