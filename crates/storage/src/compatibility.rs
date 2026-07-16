use crate::format::{write_atomic, DATA_FORMAT_VERSION};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::Path;

const COMPATIBILITY_FILE: &str = "COMPATIBILITY";
pub const BASE_STORAGE_FEATURE_LEVEL: u32 = 1;
pub const MAX_READER_FEATURE_LEVEL: u32 = 1;
pub const MAX_WRITER_FEATURE_LEVEL: u32 = 1;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct BinaryCapabilities {
    pub binary_version: String,
    pub data_format: u32,
    pub minimum_reader_feature_level: u32,
    pub maximum_reader_feature_level: u32,
    pub maximum_writer_feature_level: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CompatibilityState {
    pub data_format: u32,
    pub active_writer_feature_level: u32,
    pub minimum_reader_feature_level: u32,
    pub generation: u64,
}

pub fn binary_capabilities() -> BinaryCapabilities {
    BinaryCapabilities {
        binary_version: env!("CARGO_PKG_VERSION").into(),
        data_format: DATA_FORMAT_VERSION,
        minimum_reader_feature_level: BASE_STORAGE_FEATURE_LEVEL,
        maximum_reader_feature_level: MAX_READER_FEATURE_LEVEL,
        maximum_writer_feature_level: MAX_WRITER_FEATURE_LEVEL,
    }
}

pub fn prepare_compatibility(
    root: &Path,
    requested_writer_feature_level: u32,
) -> io::Result<CompatibilityState> {
    prepare_with_capabilities(root, requested_writer_feature_level, &binary_capabilities())
}

pub fn read_compatibility(root: &Path) -> io::Result<Option<CompatibilityState>> {
    match fs::read(root.join(COMPATIBILITY_FILE)) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(io::Error::other),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn prepare_with_capabilities(
    root: &Path,
    requested: u32,
    capabilities: &BinaryCapabilities,
) -> io::Result<CompatibilityState> {
    if requested < capabilities.minimum_reader_feature_level
        || requested > capabilities.maximum_writer_feature_level
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "requested storage feature level {requested} is outside binary writer range {}..={}",
                capabilities.minimum_reader_feature_level,
                capabilities.maximum_writer_feature_level
            ),
        ));
    }
    let path = root.join(COMPATIBILITY_FILE);
    let mut state = read_compatibility(root)?.unwrap_or(CompatibilityState {
        data_format: DATA_FORMAT_VERSION,
        active_writer_feature_level: BASE_STORAGE_FEATURE_LEVEL,
        minimum_reader_feature_level: BASE_STORAGE_FEATURE_LEVEL,
        generation: 1,
    });
    if state.data_format != DATA_FORMAT_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "compatibility state has the wrong data format",
        ));
    }
    if state.minimum_reader_feature_level > capabilities.maximum_reader_feature_level {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "rollback fence requires reader feature level {}, binary supports only {}",
                state.minimum_reader_feature_level, capabilities.maximum_reader_feature_level
            ),
        ));
    }
    if requested < state.active_writer_feature_level {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "rollback fence forbids lowering storage feature level from {} to {requested}",
                state.active_writer_feature_level
            ),
        ));
    }
    if requested > state.active_writer_feature_level {
        state.active_writer_feature_level = requested;
        state.minimum_reader_feature_level = state.minimum_reader_feature_level.max(requested);
        state.generation = state.generation.saturating_add(1);
    }
    write_atomic(
        &path,
        &serde_json::to_vec_pretty(&state).map_err(io::Error::other)?,
    )?;
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn capabilities(level: u32) -> BinaryCapabilities {
        BinaryCapabilities {
            binary_version: "test".into(),
            data_format: DATA_FORMAT_VERSION,
            minimum_reader_feature_level: 1,
            maximum_reader_feature_level: level,
            maximum_writer_feature_level: level,
        }
    }

    #[test]
    fn activation_persists_a_rollback_fence() {
        let root = tempdir().unwrap();
        let state = prepare_with_capabilities(root.path(), 2, &capabilities(2)).unwrap();
        assert_eq!(state.active_writer_feature_level, 2);
        assert_eq!(state.minimum_reader_feature_level, 2);

        let error = prepare_with_capabilities(root.path(), 1, &capabilities(2)).unwrap_err();
        assert!(error.to_string().contains("forbids lowering"));
        let error = prepare_with_capabilities(root.path(), 1, &capabilities(1)).unwrap_err();
        assert!(error
            .to_string()
            .contains("requires reader feature level 2"));
    }

    #[test]
    fn base_level_is_created_idempotently() {
        let root = tempdir().unwrap();
        let first = prepare_compatibility(root.path(), 1).unwrap();
        let second = prepare_compatibility(root.path(), 1).unwrap();
        assert_eq!(first, second);
    }
}
