#[path = "segment/append.rs"]
mod append;
#[path = "segment/files.rs"]
mod files;
#[path = "segment/maintenance.rs"]
mod maintenance;
#[path = "segment/metadata.rs"]
mod metadata;
#[path = "segment/recovery_index.rs"]
mod recovery_index;
#[path = "segment/scrub.rs"]
mod scrub;

use crate::{crash_failpoint, Record, RecordHeader, BASE_STORAGE_FEATURE_LEVEL, HEADER_LEN};
use append::write_parts;
use files::{
    read_record, scan_segment, segment_base_index, segment_path, segment_paths, visit_record,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use thiserror::Error;

pub use metadata::RecoveryMetadataRef;
pub use scrub::{ScrubKind, ScrubTarget};

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
    /// Locations are resident only for the active segment and for a sealed
    /// segment whose rebuildable sidecar has not yet been persisted.
    resident_records: Vec<RecordLocation>,
    sealed_indexes: BTreeMap<PathBuf, recovery_index::RecoveryIndexSummary>,
    current_path: PathBuf,
    current_segment: Arc<PathBuf>,
    current: File,
    current_len: u64,
    recovery: RecoveryReport,
    start_index: u64,
    first_index: Option<u64>,
    last_index: Option<u64>,
    storage_bytes: u64,
    checksums: BTreeMap<PathBuf, (u64, u32)>,
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

        let mut resident_records = Vec::new();
        let mut sealed_indexes = BTreeMap::new();
        let mut checksums = BTreeMap::new();
        let mut recovery = RecoveryReport::default();
        let last_path = paths.last().cloned().expect("at least one segment");
        let mut expected = None;
        let mut first_index = None;
        let mut storage_bytes = 0u64;

        for path in &paths {
            let is_last = path == &last_path;
            let indexed = if is_last {
                None
            } else {
                recovery_index::load_summary(path)
                    .ok()
                    .flatten()
                    .filter(|index| {
                        fs::metadata(path)
                            .map(|metadata| metadata.len() == index.segment_len)
                            .unwrap_or(false)
                    })
            };
            let (first, last, count, truncated, bytes, crc32c) = if let Some(index) = indexed {
                let count = usize::try_from(index.location_count).unwrap_or(usize::MAX);
                recovery.indexed_records = recovery.indexed_records.saturating_add(count);
                let first = (index.location_count > 0).then_some(index.first_index);
                let last = (index.location_count > 0).then_some(index.last_index);
                sealed_indexes.insert(path.clone(), index.clone());
                (
                    first,
                    last,
                    count,
                    0,
                    index.segment_len,
                    index.segment_crc32c,
                )
            } else {
                let scanned = scan_segment(path, is_last)?;
                let count = scanned.0.len();
                recovery.scanned_records = recovery.scanned_records.saturating_add(count);
                let first = scanned.0.first().map(|location| location.index);
                let last = scanned.0.last().map(|location| location.index);
                resident_records.extend(scanned.0);
                (first, last, count, scanned.1, scanned.2, scanned.3)
            };
            checksums.insert(path.clone(), (bytes, crc32c));
            storage_bytes = storage_bytes.saturating_add(bytes);
            recovery.truncated_bytes += truncated;
            if count > 0 {
                let first = first.expect("non-empty segment has first index");
                let last = last.expect("non-empty segment has last index");
                first_index.get_or_insert(first);
                if let Some(expected_index) = expected {
                    if first != expected_index {
                        return Err(StorageError::NonContiguous {
                            expected: expected_index,
                            actual: first,
                        });
                    }
                }
                let expected_last = first.saturating_add(count as u64).saturating_sub(1);
                if last != expected_last {
                    return Err(StorageError::NonContiguous {
                        expected: expected_last,
                        actual: last,
                    });
                }
                expected = Some(last.saturating_add(1));
                recovery.last_index = last;
            }
            recovery.records = recovery.records.saturating_add(count);
        }

        if recovery.records == 0 {
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
        let last_index = (recovery.records > 0).then_some(recovery.last_index);
        Ok(Self {
            directory,
            max_segment_bytes: max_segment_bytes.max(HEADER_LEN as u64 + 1),
            resident_records,
            sealed_indexes,
            current_path,
            current_segment,
            current,
            current_len,
            recovery,
            start_index: expected.unwrap_or(start_index),
            first_index,
            last_index,
            storage_bytes,
            checksums,
            isolated: AtomicBool::new(false),
            active_writer_feature_level,
        })
    }

    pub fn recovery_report(&self) -> &RecoveryReport {
        &self.recovery
    }

    pub fn first_index(&self) -> Option<u64> {
        self.first_index
    }

    pub fn last_index(&self) -> Option<u64> {
        self.last_index
    }

    pub fn next_index(&self) -> u64 {
        self.last_index()
            .map_or(self.start_index, |index| index + 1)
    }

    /// Returns the current on-disk footprint using the checksums maintained by
    /// append, recovery, rotation and purge, including recovery sidecars. This
    /// does not touch the filesystem.
    pub fn storage_usage(&self) -> (u64, u64) {
        let sidecar_bytes = self
            .sealed_indexes
            .values()
            .map(|index| index.metadata_offset.saturating_add(index.metadata_len))
            .fold(0u64, u64::saturating_add);
        (
            self.checksums.len() as u64,
            self.storage_bytes.saturating_add(sidecar_bytes),
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
        let payload_len = payload
            .iter()
            .try_fold(0usize, |total, part| total.checked_add(part.len()))
            .ok_or_else(|| {
                StorageError::Io(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "record payload length overflow",
                ))
            })?;
        let required = record.kind.required_writer_feature_level(payload_len);
        if required > self.active_writer_feature_level {
            return Err(StorageError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "record kind {:?} with {payload_len} payload bytes requires writer feature level {required}, active level is {}",
                    record.kind,
                    self.active_writer_feature_level
                ),
            )));
        }
        if self.last_index().is_none() && self.current_len == 0 && record.index >= self.start_index
        {
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
        self.storage_bytes = self.storage_bytes.saturating_add(encoded_len);
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
        self.resident_records.push(location.clone());
        self.first_index.get_or_insert(record.index);
        self.last_index = Some(record.index);
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

    pub fn clone_current_for_sync(&self) -> Result<File, StorageError> {
        self.ensure_available()?;
        let result = self.current.try_clone().map_err(StorageError::from);
        if result.is_err() {
            self.isolate();
        }
        result
    }

    pub fn mark_sync_failed(&self) {
        self.isolate();
    }

    pub fn read(&self, index: u64) -> Result<Option<Record>, StorageError> {
        let Some(location) = self.location(index)? else {
            return Ok(None);
        };
        self.read_location(&location).map(Some)
    }

    pub fn read_location(&self, location: &RecordLocation) -> Result<Record, StorageError> {
        let result = read_record(location);
        self.observe_read(result)
    }

    pub fn read_location_with<T>(
        &self,
        location: &RecordLocation,
        visitor: impl FnOnce(RecordHeader, usize, &mut dyn io::Read) -> io::Result<T>,
    ) -> Result<T, StorageError> {
        self.observe_read(visit_record(location, visitor))
    }

    pub fn read_all(&self) -> Result<Vec<Record>, StorageError> {
        self.all_locations()?
            .iter()
            .map(|location| self.read_location(location))
            .collect()
    }

    pub fn read_all_with_locations(&self) -> Result<Vec<(RecordLocation, Record)>, StorageError> {
        self.all_locations()?
            .into_iter()
            .map(|location| {
                let record = self.read_location(&location)?;
                Ok((location, record))
            })
            .collect()
    }

    pub fn location(&self, index: u64) -> Result<Option<RecordLocation>, StorageError> {
        if let Some(location) = self
            .resident_records
            .iter()
            .find(|location| location.index == index)
            .cloned()
        {
            return Ok(Some(location));
        }
        let Some((path, summary)) = self
            .sealed_indexes
            .iter()
            .find(|(_, summary)| index >= summary.first_index && index <= summary.last_index)
        else {
            return Ok(None);
        };
        recovery_index::load_location(path, summary, index)
    }

    pub fn locations_for_segment(&self, path: &Path) -> Result<Vec<RecordLocation>, StorageError> {
        if let Some(summary) = self.sealed_indexes.get(path) {
            return recovery_index::load_locations(path, summary);
        }
        Ok(self
            .resident_records
            .iter()
            .filter(|location| location.segment.as_ref() == path)
            .cloned()
            .collect())
    }

    pub fn current_segment_path(&self) -> &Path {
        &self.current_path
    }

    pub fn recovery_metadata_ref(&self, path: &Path) -> Option<RecoveryMetadataRef> {
        let summary = self.sealed_indexes.get(path)?;
        Some(RecoveryMetadataRef {
            segment: Arc::new(path.to_path_buf()),
            index: Arc::new(recovery_index::index_path(path)),
            offset: summary.metadata_offset,
            len: summary.metadata_len,
            segment_len: summary.segment_len,
        })
    }

    pub fn load_recovery_metadata(&self, path: &Path) -> Result<Option<Vec<u8>>, StorageError> {
        self.sealed_indexes
            .get(path)
            .map(|summary| recovery_index::load_metadata(path, summary))
            .transpose()
    }

    pub fn record_index_range(&self, path: &Path) -> Option<(u64, u64)> {
        if let Some(index) = self.sealed_indexes.get(path) {
            return (index.location_count > 0).then_some((index.first_index, index.last_index));
        }
        let mut locations = self
            .resident_records
            .iter()
            .filter(|location| location.segment.as_ref() == path);
        let first = locations.next()?.index;
        let last = locations
            .next_back()
            .map_or(first, |location| location.index);
        Some((first, last))
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
        let locations = self.locations_for_segment(path)?;
        recovery_index::store(path, segment_len, segment_crc32c, &locations, &metadata)?;
        let summary = recovery_index::load_summary(path)?.ok_or_else(|| {
            StorageError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "persisted recovery index disappeared",
            ))
        })?;
        self.sealed_indexes.insert(path.to_path_buf(), summary);
        self.resident_records
            .retain(|location| location.segment.as_ref() != path);
        Ok(())
    }

    fn all_locations(&self) -> Result<Vec<RecordLocation>, StorageError> {
        let mut output = Vec::with_capacity(self.recovery.records);
        for path in segment_paths(&self.directory)? {
            output.extend(self.locations_for_segment(&path)?);
        }
        output.sort_by_key(|location| location.index);
        Ok(output)
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

    fn refresh_aggregates(&mut self) {
        self.first_index = self
            .sealed_indexes
            .values()
            .filter(|index| index.location_count > 0)
            .map(|index| index.first_index)
            .chain(self.resident_records.iter().map(|record| record.index))
            .min();
        self.last_index = self
            .sealed_indexes
            .values()
            .filter(|index| index.location_count > 0)
            .map(|index| index.last_index)
            .chain(self.resident_records.iter().map(|record| record.index))
            .max();
        self.storage_bytes = self
            .checksums
            .values()
            .map(|(bytes, _)| *bytes)
            .fold(0u64, u64::saturating_add);
    }
}

#[cfg(test)]
mod tests;
