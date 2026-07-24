use super::{RecordLocation, StorageError};
use crate::{Record, RecordHeader, HEADER_LEN};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const SEGMENT_PREFIX: &str = "segment-";
const SEGMENT_SUFFIX: &str = ".rqlog";

pub(super) fn read_record(location: &RecordLocation) -> Result<Record, StorageError> {
    visit_record(location, |header, payload_len, reader| {
        let mut payload = Vec::with_capacity(payload_len);
        reader.read_to_end(&mut payload)?;
        Ok(Record {
            kind: header.kind,
            flags: header.flags,
            index: header.index,
            timestamp_ns: header.timestamp_ns,
            message_id: header.message_id,
            available_at_ms: header.available_at_ms,
            payload,
        })
    })
}

pub(super) fn visit_record<T>(
    location: &RecordLocation,
    visitor: impl FnOnce(RecordHeader, usize, &mut dyn Read) -> io::Result<T>,
) -> Result<T, StorageError> {
    let mut file = File::open(location.segment.as_ref())?;
    file.seek(SeekFrom::Start(location.offset))?;
    let mut header = [0u8; HEADER_LEN];
    file.read_exact(&mut header)?;
    let payload_len = Record::payload_len(&header)?;
    if location.encoded_len != HEADER_LEN as u64 + payload_len as u64 {
        return Err(corrupt(
            location.segment.as_ref(),
            location.offset,
            "record location length does not match its header",
        ));
    }
    let decoded = RecordHeader::decode(&header)?;
    if decoded.index != location.index {
        return Err(corrupt(
            location.segment.as_ref(),
            location.offset,
            "recovery index points at a different record",
        ));
    }
    let expected_crc = u32::from_be_bytes(header[44..48].try_into().unwrap());
    let mut reader = ChecksummedReader {
        inner: file.take(payload_len as u64),
        crc32c: crc32c::crc32c(&header[..44]),
        bytes_read: 0,
    };
    let value = visitor(decoded, payload_len, &mut reader).map_err(|error| {
        corrupt(
            location.segment.as_ref(),
            location.offset,
            error.to_string(),
        )
    })?;
    let mut buffer = [0u8; 64 * 1024];
    while reader.bytes_read < payload_len {
        let read = reader.read(&mut buffer).map_err(StorageError::Io)?;
        if read == 0 {
            return Err(corrupt(
                location.segment.as_ref(),
                location.offset,
                "partial record payload",
            ));
        }
    }
    if reader.crc32c != expected_crc {
        return Err(corrupt(
            location.segment.as_ref(),
            location.offset,
            "record checksum mismatch",
        ));
    }
    Ok(value)
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
        let decoded = RecordHeader::decode(&header)
            .map_err(|error| corrupt(path, offset, error.to_string()))?;
        checksum = crc32c::crc32c_append(checksum, &header);
        let mut record_crc = crc32c::crc32c(&header[..44]);
        let mut remaining = payload_len;
        let mut buffer = [0u8; 64 * 1024];
        while remaining > 0 {
            let wanted = remaining.min(buffer.len());
            file.read_exact(&mut buffer[..wanted])?;
            checksum = crc32c::crc32c_append(checksum, &buffer[..wanted]);
            record_crc = crc32c::crc32c_append(record_crc, &buffer[..wanted]);
            remaining -= wanted;
        }
        let expected_crc = u32::from_be_bytes(header[44..48].try_into().unwrap());
        if record_crc != expected_crc {
            return Err(corrupt(path, offset, "record checksum mismatch"));
        }
        locations.push(RecordLocation {
            index: decoded.index,
            segment: Arc::clone(&segment),
            offset,
            encoded_len: record_len,
        });
        offset += record_len;
    }
    Ok((locations, 0, offset, checksum))
}

struct ChecksummedReader<R> {
    inner: R,
    crc32c: u32,
    bytes_read: usize,
}

impl<R: Read> Read for ChecksummedReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buffer)?;
        if read > 0 {
            self.crc32c = crc32c::crc32c_append(self.crc32c, &buffer[..read]);
            self.bytes_read = self.bytes_read.saturating_add(read);
        }
        Ok(read)
    }
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
