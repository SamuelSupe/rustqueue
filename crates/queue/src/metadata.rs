use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct BrokerMeta {
    pub format: u8,
    pub node_id: u64,
    pub next_sequence: u64,
    #[serde(default)]
    pub registry_revision: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct TopicManifest {
    pub format: u8,
    pub name: String,
    pub paused: bool,
    #[serde(default)]
    pub deleted: bool,
    pub next_position: u64,
}

pub(crate) fn load_optional<T: DeserializeOwned>(path: &Path) -> io::Result<Option<T>> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(io::Error::other),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

pub(crate) fn store_atomic<T: Serialize>(path: &Path, value: &T) -> io::Result<()> {
    store_bytes_atomic(path, &serde_json::to_vec(value).map_err(io::Error::other)?)
}

pub(crate) fn store_bytes_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = path.parent().expect("metadata path has parent");
    fs::create_dir_all(parent)?;
    let temporary = temporary_path(path);
    let mut file = File::create(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    File::open(parent)?.sync_all()
}

pub(crate) fn topic_directory(root: &Path, topic: &str) -> PathBuf {
    root.join("topics").join(hex::encode(topic.as_bytes()))
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".tmp");
    path.with_file_name(name)
}
