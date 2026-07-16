use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

pub const DATA_FORMAT_VERSION: u32 = 7;
const FORMAT_FILE: &str = "FORMAT";

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DataFormat {
    pub version: u32,
}

pub fn ensure_data_format(root: &Path) -> io::Result<DataFormat> {
    fs::create_dir_all(root)?;
    let path = root.join(FORMAT_FILE);
    match fs::read(&path) {
        Ok(bytes) => {
            let format: DataFormat = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
            if format.version != DATA_FORMAT_VERSION {
                return Err(incompatible(format.version));
            }
            Ok(format)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if contains_legacy_layout(root)? {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "RustQueue 0.7 requires an empty format-v7 data directory; older data cannot be migrated in place",
                ));
            }
            let format = DataFormat {
                version: DATA_FORMAT_VERSION,
            };
            write_atomic(
                &path,
                &serde_json::to_vec_pretty(&format).map_err(io::Error::other)?,
            )?;
            Ok(format)
        }
        Err(error) => Err(error),
    }
}

pub fn read_data_format(root: &Path) -> io::Result<Option<DataFormat>> {
    let path = root.join(FORMAT_FILE);
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(io::Error::other),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn contains_legacy_layout(root: &Path) -> io::Result<bool> {
    let legacy = [
        "rustqueue-format.json",
        "broker.meta",
        "catalog.json",
        "topics",
        "consensus",
    ];
    Ok(legacy.iter().any(|name| root.join(name).exists()))
}

fn incompatible(actual: u32) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("unsupported RustQueue data format {actual}; expected {DATA_FORMAT_VERSION}"),
    )
}

pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "format path has no parent"))?;
    let temporary: PathBuf = parent.join(format!(".{FORMAT_FILE}.tmp"));
    let mut file = File::create(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    File::open(parent)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn initializes_and_reopens_v7() {
        let directory = tempdir().unwrap();
        assert_eq!(
            ensure_data_format(directory.path()).unwrap().version,
            DATA_FORMAT_VERSION
        );
        assert_eq!(
            read_data_format(directory.path()).unwrap().unwrap().version,
            DATA_FORMAT_VERSION
        );
    }

    #[test]
    fn refuses_legacy_layout_without_marker() {
        let directory = tempdir().unwrap();
        fs::write(directory.path().join("catalog.json"), b"{}").unwrap();
        let error = ensure_data_format(directory.path()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn refuses_pre_v7_format() {
        let directory = tempdir().unwrap();
        fs::write(directory.path().join(FORMAT_FILE), br#"{"version":2}"#).unwrap();
        let error = ensure_data_format(directory.path()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("expected 7"));
    }
}
