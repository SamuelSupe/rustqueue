use super::{RecordLocation, StorageError};
use crate::{Record, HEADER_LEN};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const SEGMENT_PREFIX: &str = "segment-";
const SEGMENT_SUFFIX: &str = ".rqlog";

pub(super) fn read_record(location: &RecordLocation) -> Result<Record, StorageError> {
    let mut file = File::open(location.segment.as_ref())?;
    file.seek(SeekFrom::Start(location.offset))?;
    let mut header = [0u8; HEADER_LEN];
    file.read_exact(&mut header)?;
    let payload_len = Record::payload_len(&header)?;
    let mut payload = vec![0; payload_len];
    file.read_exact(&mut payload)?;
    Record::decode(&header, payload).map_err(StorageError::Io)
}

pub(super) fn scan_segment(
    path: &Path,
    allow_tail_repair: bool,
) -> Result<(Vec<RecordLocation>, u64, u64, u32), StorageError> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(allow_tail_repair)
        .open(path)?;
    let original_len = file.metadata()?.len();
    let mut offset = 0u64;
    let mut locations = Vec::new();
    let mut checksum = 0u32;
    let segment = Arc::new(path.to_path_buf());

    while offset < original_len {
        let remaining = original_len - offset;
        if remaining < HEADER_LEN as u64 {
            if allow_tail_repair {
                file.set_len(offset)?;
                file.sync_all()?;
                return Ok((locations, original_len - offset, offset, checksum));
            }
            return Err(corrupt(path, offset, "partial record header"));
        }

        file.seek(SeekFrom::Start(offset))?;
        let mut header = [0u8; HEADER_LEN];
        file.read_exact(&mut header)?;
        let payload_len = Record::payload_len(&header)
            .map_err(|error| corrupt(path, offset, error.to_string()))?;
        let record_len = HEADER_LEN as u64 + payload_len as u64;
        if remaining < record_len {
            if allow_tail_repair {
                file.set_len(offset)?;
                file.sync_all()?;
                return Ok((locations, original_len - offset, offset, checksum));
            }
            return Err(corrupt(path, offset, "partial record payload"));
        }
        let mut payload = vec![0; payload_len];
        file.read_exact(&mut payload)?;
        let record_checksum =
            crc32c::crc32c_append(crc32c::crc32c_append(checksum, &header), &payload);
        match Record::decode(&header, payload) {
            Ok(record) => {
                checksum = record_checksum;
                locations.push(RecordLocation {
                    index: record.index,
                    segment: Arc::clone(&segment),
                    offset,
                    encoded_len: record_len,
                });
            }
            Err(error) => return Err(corrupt(path, offset, error.to_string())),
        }
        offset += record_len;
    }
    Ok((locations, 0, offset, checksum))
}

fn corrupt(path: &Path, offset: u64, reason: impl Into<String>) -> StorageError {
    StorageError::Corrupt {
        path: path.to_path_buf(),
        offset,
        reason: reason.into(),
    }
}

pub(super) fn segment_paths(directory: &Path) -> io::Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !name.starts_with(SEGMENT_PREFIX) || !name.ends_with(SEGMENT_SUFFIX) {
            continue;
        }
        if segment_base_index(&path).is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid segment filename {name}"),
            ));
        }
        paths.push(path);
    }
    paths.sort();
    Ok(paths)
}

pub(super) fn segment_path(directory: &Path, base_index: u64) -> PathBuf {
    directory.join(format!("{SEGMENT_PREFIX}{base_index:020}{SEGMENT_SUFFIX}"))
}

pub(super) fn segment_base_index(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let index = name
        .strip_prefix(SEGMENT_PREFIX)?
        .strip_suffix(SEGMENT_SUFFIX)?;
    if index.len() != 20 || !index.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    index.parse().ok()
}
