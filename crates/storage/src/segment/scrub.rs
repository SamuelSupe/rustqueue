use super::StorageError;
use crate::{Record, RecordKind, HEADER_LEN};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::time::{Duration, Instant};

const BUFFER_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScrubTarget {
    pub path: PathBuf,
    pub expected_len: u64,
    pub expected_crc32c: u32,
}

pub(super) fn verify(target: &ScrubTarget, bytes_per_second: u64) -> Result<usize, StorageError> {
    let mut file = File::open(&target.path)?;
    let actual_len = file.metadata()?.len();
    if actual_len != target.expected_len {
        return Err(corrupt(
            target,
            0,
            format!(
                "segment length changed: expected {}, got {actual_len}",
                target.expected_len
            ),
        ));
    }
    file.seek(SeekFrom::Start(0))?;
    let mut limiter = RateLimiter::new(bytes_per_second);
    let mut offset = 0u64;
    let mut records = 0usize;
    let mut segment_crc = 0u32;
    let mut buffer = vec![0u8; BUFFER_BYTES];
    while offset < actual_len {
        if actual_len - offset < HEADER_LEN as u64 {
            return Err(corrupt(target, offset, "partial record header"));
        }
        let mut header = [0u8; HEADER_LEN];
        file.read_exact(&mut header)?;
        limiter.consume(HEADER_LEN as u64);
        let payload_len = Record::payload_len(&header)
            .map_err(|error| corrupt(target, offset, error.to_string()))?;
        RecordKind::try_from(header[5])
            .map_err(|error| corrupt(target, offset, error.to_string()))?;
        let record_len = HEADER_LEN as u64 + payload_len as u64;
        if actual_len - offset < record_len {
            return Err(corrupt(target, offset, "partial record payload"));
        }
        segment_crc = crc32c::crc32c_append(segment_crc, &header);
        let mut record_crc = crc32c::crc32c(&header[..44]);
        let mut remaining = payload_len;
        while remaining > 0 {
            let wanted = remaining.min(buffer.len());
            file.read_exact(&mut buffer[..wanted])?;
            record_crc = crc32c::crc32c_append(record_crc, &buffer[..wanted]);
            segment_crc = crc32c::crc32c_append(segment_crc, &buffer[..wanted]);
            remaining -= wanted;
            limiter.consume(wanted as u64);
        }
        let expected = u32::from_be_bytes(header[44..48].try_into().unwrap());
        if record_crc != expected {
            return Err(corrupt(target, offset, "record checksum mismatch"));
        }
        offset += record_len;
        records += 1;
    }
    if segment_crc != target.expected_crc32c {
        return Err(corrupt(target, 0, "segment checksum mismatch"));
    }
    Ok(records)
}

fn corrupt(target: &ScrubTarget, offset: u64, reason: impl Into<String>) -> StorageError {
    StorageError::Corrupt {
        path: target.path.clone(),
        offset,
        reason: reason.into(),
    }
}

struct RateLimiter {
    bytes_per_second: u64,
    started: Instant,
    consumed: u64,
}

impl RateLimiter {
    fn new(bytes_per_second: u64) -> Self {
        Self {
            bytes_per_second,
            started: Instant::now(),
            consumed: 0,
        }
    }

    fn consume(&mut self, bytes: u64) {
        if self.bytes_per_second == 0 {
            return;
        }
        self.consumed = self.consumed.saturating_add(bytes);
        let expected = Duration::from_secs_f64(self.consumed as f64 / self.bytes_per_second as f64);
        if let Some(delay) = expected.checked_sub(self.started.elapsed()) {
            std::thread::sleep(delay);
        }
    }
}
