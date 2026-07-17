#[path = "segment/append.rs"]
mod append;
#[path = "segment/files.rs"]
mod files;
#[path = "segment/maintenance.rs"]
mod maintenance;
#[path = "segment/recovery_index.rs"]
mod recovery_index;
#[path = "segment/scrub.rs"]
mod scrub;

use crate::{crash_failpoint, Record, RecordHeader, BASE_STORAGE_FEATURE_LEVEL, HEADER_LEN};
use append::write_parts;
use files::{read_record, scan_segment, segment_base_index, segment_path, segment_paths};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use thiserror::Error;

pub use scrub::ScrubTarget;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("storage I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("corrupt segment {path} at byte {offset}: {reason}")]
    Corrupt {
        path: PathBuf,
        offset: u64,
        reason: String,
    },
    #[error("log index is not contiguous: expected {expected}, got {actual}")]
    NonContiguous { expected: u64, actual: u64 },
    #[error("segment log is isolated after an earlier storage failure")]
    Isolated,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordLocation {
    pub index: u64,
    pub segment: Arc<PathBuf>,
    pub offset: u64,
    pub encoded_len: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    pub records: usize,
    pub truncated_bytes: u64,
    pub last_index: u64,
    pub indexed_records: usize,
    pub scanned_records: usize,
}

pub struct SegmentLog {
    directory: PathBuf,
    max_segment_bytes: u64,
    records: Vec<RecordLocation>,
    current_path: PathBuf,
    current_segment: Arc<PathBuf>,
    current: File,
    current_len: u64,
    recovery: RecoveryReport,
    start_index: u64,
    checksums: BTreeMap<PathBuf, (u64, u32)>,
    recovery_metadata: BTreeMap<PathBuf, Vec<u8>>,
    isolated: AtomicBool,
    active_writer_feature_level: u32,
}

impl SegmentLog {
    pub fn open(directory: impl AsRef<Path>, max_segment_bytes: u64) -> Result<Self, StorageError> {
        Self::open_with_feature_level(directory, max_segment_bytes, 1, BASE_STORAGE_FEATURE_LEVEL)
    }

    pub fn open_with_start_index(
        directory: impl AsRef<Path>,
        max_segment_bytes: u64,
        start_index: u64,
    ) -> Result<Self, StorageError> {
        Self::open_with_feature_level(
            directory,
            max_segment_bytes,
            start_index,
            BASE_STORAGE_FEATURE_LEVEL,
        )
    }

    pub fn open_with_feature_level(
        directory: impl AsRef<Path>,
        max_segment_bytes: u64,
        start_index: u64,
        active_writer_feature_level: u32,
    ) -> Result<Self, StorageError> {
        fs::create_dir_all(directory.as_ref())?;
        let directory = directory.as_ref().to_path_buf();
        let mut paths = segment_paths(&directory)?;
        if paths.is_empty() {
            paths.push(segment_path(&directory, start_index));
            File::create(&paths[0])?.sync_all()?;
            File::open(&directory)?.sync_all()?;
        }

        let mut records = Vec::new();
        let mut checksums = BTreeMap::new();
        let mut recovery_metadata = BTreeMap::new();
        let mut recovery = RecoveryReport::default();
        let last_path = paths.last().cloned().expect("at least one segment");
        let mut expected = None;

        for path in &paths {
            let is_last = path == &last_path;
            let indexed = if is_last {
                None
            } else {
                recovery_index::load(path).ok().flatten().filter(|index| {
                    fs::metadata(path)
                        .map(|metadata| metadata.len() == index.segment_len)
                        .unwrap_or(false)
                })
            };
            let (locations, truncated, bytes, crc32c) = if let Some(index) = indexed {
                recovery.indexed_records += index.locations.len();
                recovery_metadata.insert(path.clone(), index.metadata);
                (index.locations, 0, index.segment_len, index.segment_crc32c)
            } else {
                let scanned = scan_segment(path, is_last)?;
                recovery.scanned_records += scanned.0.len();
                scanned
            };
            checksums.insert(path.clone(), (bytes, crc32c));
            recovery.truncated_bytes += truncated;
            for location in locations {
                if let Some(expected_index) = expected {
                    if location.index != expected_index {
                        return Err(StorageError::NonContiguous {
                            expected: expected_index,
                            actual: location.index,
                        });
                    }
                }
                expected = Some(location.index + 1);
                recovery.last_index = location.index;
                records.push(location);
            }
        }
        recovery.records = records.len();

        if records.is_empty() {
            if let Some(base_index) = segment_base_index(&last_path) {
                expected = Some(base_index);
            }
        }

        let current_path = last_path;
        let current_segment = Arc::new(current_path.clone());
        let current = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&current_path)?;
        let current_len = current.metadata()?.len();
        Ok(Self {
            directory,
            max_segment_bytes: max_segment_bytes.max(HEADER_LEN as u64 + 1),
            records,
            current_path,
            current_segment,
            current,
            current_len,
            recovery,
            start_index: expected.unwrap_or(start_index),
            checksums,
            recovery_metadata,
            isolated: AtomicBool::new(false),
            active_writer_feature_level,
        })
    }

    pub fn recovery_report(&self) -> &RecoveryReport {
        &self.recovery
    }

    pub fn first_index(&self) -> Option<u64> {
        self.records.first().map(|record| record.index)
    }

    pub fn last_index(&self) -> Option<u64> {
        self.records.last().map(|record| record.index)
    }

    pub fn next_index(&self) -> u64 {
        self.last_index()
            .map_or(self.start_index, |index| index + 1)
    }

    /// Returns the current on-disk footprint using the checksums maintained by
    /// append, recovery, rotation and purge. This does not touch the filesystem.
    pub fn storage_usage(&self) -> (u64, u64) {
        (
            self.checksums.len() as u64,
            self.checksums
                .values()
                .map(|(bytes, _)| *bytes)
                .fold(0u64, u64::saturating_add),
        )
    }

    pub fn append(&mut self, mut record: Record, durable: bool) -> Result<u64, StorageError> {
        record.index = self.next_index();
        self.append_at_with_location(record, durable)
            .map(|location| location.index)
    }

    pub fn append_at(&mut self, record: Record, durable: bool) -> Result<u64, StorageError> {
        self.append_at_with_location(record, durable)
            .map(|location| location.index)
    }

    pub fn append_at_with_location(
        &mut self,
        record: Record,
        durable: bool,
    ) -> Result<RecordLocation, StorageError> {
        let header = record.header();
        self.append_parts_at_with_location(header, &[record.payload.as_slice()], durable)
    }

    pub fn append_parts_at_with_location(
        &mut self,
        record: RecordHeader,
        payload: &[&[u8]],
        durable: bool,
    ) -> Result<RecordLocation, StorageError> {
        self.ensure_available()?;
        let required = record.kind.required_writer_feature_level();
        if required > self.active_writer_feature_level {
            return Err(StorageError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "record kind {:?} requires writer feature level {required}, active level is {}",
                    record.kind, self.active_writer_feature_level
                ),
            )));
        }
        if self.records.is_empty() && self.current_len == 0 && record.index >= self.start_index {
            self.start_index = record.index;
        }
        let expected = self.next_index();
        if record.index != expected {
            return Err(StorageError::NonContiguous {
                expected,
                actual: record.index,
            });
        }
        let header = record.encode(payload)?;
        let encoded_len = payload.iter().try_fold(HEADER_LEN as u64, |total, part| {
            total.checked_add(part.len() as u64)
        });
        let Some(encoded_len) = encoded_len else {
            return Err(StorageError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "record length overflow",
            )));
        };
        let result = self.append_parts(&record, &header, payload, encoded_len, durable);
        if result.is_err() {
            self.isolate();
        }
        result
    }

    fn append_parts(
        &mut self,
        record: &RecordHeader,
        header: &[u8; HEADER_LEN],
        payload: &[&[u8]],
        encoded_len: u64,
        durable: bool,
    ) -> Result<RecordLocation, StorageError> {
        if self.current_len > 0 && self.current_len + encoded_len > self.max_segment_bytes {
            self.rotate(record.index)?;
        }
        let offset = self.current_len;
        write_parts(&mut self.current, header, payload)?;
        if durable {
            self.current.sync_data()?;
        }
        self.current_len += encoded_len;
        let checksum = self
            .checksums
            .entry(self.current_path.clone())
            .or_insert((offset, 0));
        checksum.0 = self.current_len;
        checksum.1 = crc32c::crc32c_append(checksum.1, header);
        for part in payload {
            checksum.1 = crc32c::crc32c_append(checksum.1, part);
        }
        let location = RecordLocation {
            index: record.index,
            segment: Arc::clone(&self.current_segment),
            offset,
            encoded_len,
        };
        self.records.push(location.clone());
        Ok(location)
    }

    pub fn sync(&self) -> Result<(), StorageError> {
        self.ensure_available()?;
        let result = self.current.sync_data().map_err(StorageError::from);
        if result.is_err() {
            self.isolate();
        }
        result
    }

    pub fn read(&self, index: u64) -> Result<Option<Record>, StorageError> {
        let Some(location) = self.records.iter().find(|location| location.index == index) else {
            return Ok(None);
        };
        read_record(location).map(Some)
    }

    pub fn read_location(&self, location: &RecordLocation) -> Result<Record, StorageError> {
        self.observe_read(read_record(location))
    }

    pub fn read_all(&self) -> Result<Vec<Record>, StorageError> {
        self.records.iter().map(read_record).collect()
    }

    pub fn read_all_with_locations(&self) -> Result<Vec<(RecordLocation, Record)>, StorageError> {
        self.records
            .iter()
            .map(|location| Ok((location.clone(), read_record(location)?)))
            .collect()
    }

    pub fn location(&self, index: u64) -> Option<RecordLocation> {
        self.records
            .iter()
            .find(|location| location.index == index)
            .cloned()
    }

    pub fn locations(&self) -> &[RecordLocation] {
        &self.records
    }

    pub fn current_segment_path(&self) -> &Path {
        &self.current_path
    }

    pub fn recovery_metadata(&self, path: &Path) -> Option<&[u8]> {
        self.recovery_metadata.get(path).map(Vec::as_slice)
    }

    pub fn record_index_range(&self, path: &Path) -> Option<(u64, u64)> {
        let first = segment_base_index(path)?;
        let start = self.records.partition_point(|record| record.index < first);
        let found = self.records.get(start)?;
        if found.segment.as_ref() != path {
            return None;
        }
        let next = self
            .checksums
            .keys()
            .filter_map(|candidate| segment_base_index(candidate))
            .find(|base| *base > first)
            .unwrap_or_else(|| self.next_index());
        Some((first, next.saturating_sub(1)))
    }

    pub fn persist_recovery_index(
        &mut self,
        path: &Path,
        metadata: Vec<u8>,
    ) -> Result<(), StorageError> {
        self.ensure_available()?;
        if path == self.current_path {
            return Err(StorageError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot index the active segment",
            )));
        }
        let (segment_len, segment_crc32c) = self.checksums.get(path).copied().ok_or_else(|| {
            StorageError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                "segment checksum is unavailable",
            ))
        })?;
        let locations: Vec<_> = self
            .records
            .iter()
            .filter(|location| location.segment.as_ref() == path)
            .cloned()
            .collect();
        recovery_index::store(path, segment_len, segment_crc32c, &locations, &metadata)?;
        self.recovery_metadata.insert(path.to_path_buf(), metadata);
        Ok(())
    }

    fn ensure_available(&self) -> Result<(), StorageError> {
        if self.isolated.load(Ordering::Acquire) {
            Err(StorageError::Isolated)
        } else {
            Ok(())
        }
    }

    fn isolate(&self) {
        self.isolated.store(true, Ordering::Release);
    }

    fn observe_read<T>(&self, result: Result<T, StorageError>) -> Result<T, StorageError> {
        if result.is_err() {
            self.isolate();
        }
        result
    }

    fn rotate(&mut self, next_index: u64) -> Result<(), StorageError> {
        self.current.sync_all()?;
        let path = segment_path(&self.directory, next_index);
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .append(true)
            .open(&path)?;
        File::open(&self.directory)?.sync_all()?;
        self.current_path = path;
        self.current_segment = Arc::new(self.current_path.clone());
        self.current = file;
        self.current_len = 0;
        self.checksums.insert(self.current_path.clone(), (0, 0));
        Ok(())
    }
}

#[cfg(test)]
mod tests;
