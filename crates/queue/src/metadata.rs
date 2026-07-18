use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const MAX_METADATA_BYTES: u64 = 64 * 1024 * 1024;

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
    match read_bounded(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(io::Error::other),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

pub(crate) fn load_topic_manifest(path: &Path) -> io::Result<Option<TopicManifest>> {
    match read_bounded(path) {
        Ok(bytes) => parse_topic_manifest(&bytes).map(Some),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn parse_topic_manifest(bytes: &[u8]) -> io::Result<TopicManifest> {
    serde_json::from_slice(bytes).map_err(io::Error::other)
}

pub(crate) fn fuzz_topic_manifest(bytes: &[u8]) {
    let _ = parse_topic_manifest(bytes);
}

pub(crate) fn store_atomic<T: Serialize>(path: &Path, value: &T) -> io::Result<()> {
    store_bytes_atomic(path, &serde_json::to_vec(value).map_err(io::Error::other)?)
}

pub(crate) fn store_bytes_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    store_bytes_atomic_with_failpoint(path, bytes, None)
}

pub(crate) fn store_bytes_atomic_with_failpoint(
    path: &Path,
    bytes: &[u8],
    failpoint: Option<&str>,
) -> io::Result<()> {
    if bytes.len() as u64 > MAX_METADATA_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "metadata file exceeds the maximum size",
        ));
    }
    let parent = path.parent().expect("metadata path has parent");
    fs::create_dir_all(parent)?;
    let temporary = temporary_path(path);
    let mut file = File::create(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    if let Some(failpoint) = failpoint {
        rustqueue_storage::crash_failpoint(failpoint);
    }
    fs::rename(&temporary, path)?;
    File::open(parent)?.sync_all()
}

fn read_bounded(path: &Path) -> io::Result<Vec<u8>> {
    if fs::metadata(path)?.len() > MAX_METADATA_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "metadata file exceeds the maximum size",
        ));
    }
    fs::read(path)
}

pub(crate) fn topic_directory(root: &Path, topic: &str) -> PathBuf {
    root.join("topics").join(hex::encode(topic.as_bytes()))
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".tmp");
    path.with_file_name(name)
}
