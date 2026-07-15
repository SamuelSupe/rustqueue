use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TopicDefinition {
    pub partitions: Vec<PartitionDefinition>,
    pub key_routing_slots: Vec<u16>,
    #[serde(default)]
    pub paused: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PartitionDefinition {
    pub number: u16,
    pub slot: u16,
    pub cell_id: u64,
    pub group_id: u64,
    pub wire_incarnation: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Catalog {
    pub next_slot: u32,
    pub topics: BTreeMap<String, TopicDefinition>,
}

impl Default for Catalog {
    fn default() -> Self {
        Self {
            next_slot: 1,
            topics: BTreeMap::new(),
        }
    }
}

pub struct CatalogStore {
    path: PathBuf,
}

impl CatalogStore {
    pub fn new(data_path: &Path) -> io::Result<Self> {
        fs::create_dir_all(data_path)?;
        Ok(Self {
            path: data_path.join("catalog.json"),
        })
    }

    pub fn load(&self) -> io::Result<Catalog> {
        match fs::read(&self.path) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(io::Error::other),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Catalog::default()),
            Err(error) => Err(error),
        }
    }

    pub fn store(&self, catalog: &Catalog) -> io::Result<()> {
        let parent = self.path.parent().expect("catalog path has parent");
        let temporary = parent.join("catalog.json.tmp");
        let mut file = File::create(&temporary)?;
        file.write_all(&serde_json::to_vec_pretty(catalog).map_err(io::Error::other)?)?;
        file.sync_all()?;
        fs::rename(&temporary, &self.path)?;
        File::open(parent)?.sync_all()
    }
}
