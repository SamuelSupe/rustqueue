#[path = "segment/files.rs"]
mod files;

use crate::{Record, HEADER_LEN};
use files::{
    read_record, scan_segment, scan_segment_readonly, segment_base_index, segment_path,
    segment_paths,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use thiserror::Error;

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
    isolated: AtomicBool,
}

impl SegmentLog {
    pub fn open(directory: impl AsRef<Path>, max_segment_bytes: u64) -> Result<Self, StorageError> {
        Self::open_with_start_index(directory, max_segment_bytes, 1)
    }

    pub fn open_with_start_index(
        directory: impl AsRef<Path>,
        max_segment_bytes: u64,
        start_index: u64,
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
        let mut recovery = RecoveryReport::default();
        let last_path = paths.last().cloned().expect("at least one segment");
        let mut expected = None;

        for path in &paths {
            let is_last = path == &last_path;
            let (locations, truncated, bytes, crc32c) = scan_segment(path, is_last)?;
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
            isolated: AtomicBool::new(false),
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
        self.ensure_available()?;
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
        let encoded = record.encode()?;
        let result = self.append_encoded(&record, &encoded, durable);
        if result.is_err() {
            self.isolate();
        }
        result
    }

    fn append_encoded(
        &mut self,
        record: &Record,
        encoded: &[u8],
        durable: bool,
    ) -> Result<RecordLocation, StorageError> {
        if self.current_len > 0 && self.current_len + encoded.len() as u64 > self.max_segment_bytes
        {
            self.rotate(record.index)?;
        }
        let offset = self.current_len;
        self.current.write_all(encoded)?;
        if durable {
            self.current.sync_data()?;
        }
        self.current_len += encoded.len() as u64;
        let checksum = self
            .checksums
            .entry(self.current_path.clone())
            .or_insert((offset, 0));
        checksum.0 = self.current_len;
        checksum.1 = crc32c::crc32c_append(checksum.1, encoded);
        let location = RecordLocation {
            index: record.index,
            segment: Arc::clone(&self.current_segment),
            offset,
            encoded_len: encoded.len() as u64,
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

    pub fn truncate_suffix(&mut self, from_index: u64) -> Result<(), StorageError> {
        self.ensure_available()?;
        let result = self.truncate_suffix_inner(from_index);
        if result.is_err() {
            self.isolate();
        }
        result
    }

    fn truncate_suffix_inner(&mut self, from_index: u64) -> Result<(), StorageError> {
        let Some(first_removed) = self
            .records
            .iter()
            .position(|record| record.index >= from_index)
        else {
            return Ok(());
        };
        let location = self.records[first_removed].clone();
        let removed_paths: Vec<PathBuf> = self.records[first_removed..]
            .iter()
            .map(|item| item.segment.as_ref().clone())
            .filter(|path| path != location.segment.as_ref())
            .collect();

        OpenOptions::new()
            .write(true)
            .open(location.segment.as_ref())?
            .set_len(location.offset)?;
        for path in removed_paths {
            if path.exists() {
                fs::remove_file(&path)?;
            }
            self.checksums.remove(&path);
        }
        let (_, _, bytes, crc32c) = scan_segment(location.segment.as_ref(), true)?;
        self.checksums
            .insert(location.segment.as_ref().clone(), (bytes, crc32c));
        self.records.truncate(first_removed);
        self.current_path = location.segment.as_ref().clone();
        self.current_segment = location.segment;
        self.current = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&self.current_path)?;
        self.current_len = self.current.metadata()?.len();
        self.current.sync_all()?;
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }

    /// Removes only immutable segments whose final record is at or below `through_index`.
    /// The active segment is never removed, even when every record in it is eligible.
    pub fn purge_prefix(&mut self, through_index: u64) -> Result<usize, StorageError> {
        self.purge_prefix_retaining(through_index, &BTreeSet::new())
    }

    pub fn purge_prefix_retaining(
        &mut self,
        through_index: u64,
        retained: &BTreeSet<PathBuf>,
    ) -> Result<usize, StorageError> {
        self.ensure_available()?;
        let result = self.purge_prefix_retaining_inner(through_index, retained);
        if result.is_err() {
            self.isolate();
        }
        result
    }

    fn purge_prefix_retaining_inner(
        &mut self,
        through_index: u64,
        retained: &BTreeSet<PathBuf>,
    ) -> Result<usize, StorageError> {
        let mut removable = Vec::new();
        for path in segment_paths(&self.directory)? {
            if path == self.current_path || retained.contains(&path) {
                continue;
            }
            let last_index = self
                .records
                .iter()
                .rev()
                .find(|record| record.segment.as_ref() == &path)
                .map(|record| record.index);
            if last_index.is_some_and(|index| index <= through_index) {
                removable.push(path);
            }
        }
        if removable.is_empty() {
            return Ok(0);
        }
        self.current.sync_all()?;
        for path in &removable {
            fs::remove_file(path)?;
            self.checksums.remove(path);
        }
        self.records
            .retain(|record| !removable.iter().any(|path| path == record.segment.as_ref()));
        if let Some(first) = self.records.first() {
            self.start_index = first.index;
        } else {
            self.start_index = through_index.saturating_add(1);
        }
        File::open(&self.directory)?.sync_all()?;
        Ok(removable.len())
    }

    pub fn scrub(&self) -> Result<usize, StorageError> {
        self.ensure_available()?;
        let mut count = 0;
        for path in segment_paths(&self.directory)? {
            let locations = match scan_segment_readonly(&path) {
                Ok(locations) => locations,
                Err(error) => {
                    self.isolate();
                    return Err(error);
                }
            };
            count += locations.len();
        }
        Ok(count)
    }

    pub fn segment_paths(&self) -> Result<Vec<PathBuf>, StorageError> {
        Ok(segment_paths(&self.directory)?)
    }

    /// Seals the current segment so every existing payload path is immutable.
    pub fn seal(&mut self) -> Result<(), StorageError> {
        self.ensure_available()?;
        let result = self.seal_inner();
        if result.is_err() {
            self.isolate();
        }
        result
    }

    pub fn immutable_file(&self, path: &Path) -> Option<(u64, u32)> {
        (path != self.current_path)
            .then(|| self.checksums.get(path).copied())
            .flatten()
    }

    pub fn oldest_inactive_boundary(&self) -> Result<Option<(PathBuf, u64)>, StorageError> {
        self.oldest_inactive_boundary_retaining(&BTreeSet::new())
    }

    pub fn oldest_inactive_boundary_retaining(
        &self,
        retained: &BTreeSet<PathBuf>,
    ) -> Result<Option<(PathBuf, u64)>, StorageError> {
        for path in segment_paths(&self.directory)? {
            if path == self.current_path || retained.contains(&path) {
                continue;
            }
            if let Some(last_index) = self
                .records
                .iter()
                .rev()
                .find(|record| record.segment.as_ref() == &path)
                .map(|record| record.index)
            {
                return Ok(Some((path, last_index)));
            }
        }
        Ok(None)
    }

    fn seal_inner(&mut self) -> Result<(), StorageError> {
        self.current.sync_all()?;
        if self.current_len > 0 {
            self.rotate(self.next_index())?;
        }
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
