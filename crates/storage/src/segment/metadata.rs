use super::StorageError;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A bounded random-access view over queue-owned metadata stored in a sealed
/// segment sidecar. The sidecar is rebuildable; the segment remains the source
/// of truth.
#[derive(Clone, Debug)]
pub struct RecoveryMetadataRef {
    pub(super) segment: Arc<PathBuf>,
    pub(super) index: Arc<PathBuf>,
    pub(super) offset: u64,
    pub(super) len: u64,
    pub(super) segment_len: u64,
}

impl RecoveryMetadataRef {
    pub fn segment_path(&self) -> &Path {
        self.segment.as_ref()
    }

    pub fn segment_len(&self) -> u64 {
        self.segment_len
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn read_range(&self, offset: u64, length: usize) -> Result<Vec<u8>, StorageError> {
        let length = u64::try_from(length).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "metadata read length overflow")
        })?;
        if offset.checked_add(length).is_none_or(|end| end > self.len) {
            return Err(StorageError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "metadata read exceeds sidecar boundary",
            )));
        }
        let mut bytes = vec![0; length as usize];
        let mut file = File::open(self.index.as_ref())?;
        file.seek(SeekFrom::Start(self.offset + offset))?;
        file.read_exact(&mut bytes)?;
        Ok(bytes)
    }
}
