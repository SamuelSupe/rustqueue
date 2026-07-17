use super::{RecordLocation, StorageError};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const MAGIC: &[u8; 4] = b"RQI7";
const VERSION: u8 = 1;
const HEADER_LEN: usize = 32;
const LOCATION_LEN: usize = 24;
const MAX_INDEX_BYTES: u64 = 512 * 1024 * 1024;

pub(super) struct RecoveryIndex {
    pub segment_len: u64,
    pub segment_crc32c: u32,
    pub locations: Vec<RecordLocation>,
    pub metadata: Vec<u8>,
}

pub(super) fn load(segment: &Path) -> Result<Option<RecoveryIndex>, StorageError> {
    let path = index_path(segment);
    let file_len = match fs::metadata(&path) {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if file_len > MAX_INDEX_BYTES {
        return Err(invalid("recovery index exceeds size limit").into());
    }
    let bytes = fs::read(path)?;
    parse(segment, &bytes).map(Some).map_err(StorageError::from)
}

pub(super) fn store(
    segment: &Path,
    segment_len: u64,
    segment_crc32c: u32,
    locations: &[RecordLocation],
    metadata: &[u8],
) -> Result<(), StorageError> {
    let location_count =
        u32::try_from(locations.len()).map_err(|_| invalid("too many recovery index locations"))?;
    let metadata_len = u32::try_from(metadata.len())
        .map_err(|_| invalid("recovery index metadata is too large"))?;
    let body_len = locations
        .len()
        .checked_mul(LOCATION_LEN)
        .and_then(|len| len.checked_add(metadata.len()))
        .ok_or_else(|| invalid("recovery index length overflow"))?;
    let mut bytes = Vec::with_capacity(HEADER_LEN + body_len);
    bytes.extend_from_slice(MAGIC);
    bytes.push(VERSION);
    bytes.extend_from_slice(&[0; 3]);
    bytes.extend_from_slice(&segment_len.to_be_bytes());
    bytes.extend_from_slice(&segment_crc32c.to_be_bytes());
    bytes.extend_from_slice(&location_count.to_be_bytes());
    bytes.extend_from_slice(&metadata_len.to_be_bytes());
    bytes.extend_from_slice(&0u32.to_be_bytes());
    for location in locations {
        bytes.extend_from_slice(&location.index.to_be_bytes());
        bytes.extend_from_slice(&location.offset.to_be_bytes());
        bytes.extend_from_slice(&location.encoded_len.to_be_bytes());
    }
    bytes.extend_from_slice(metadata);
    let checksum = crc32c::crc32c(&bytes[HEADER_LEN..]);
    bytes[28..32].copy_from_slice(&checksum.to_be_bytes());

    let path = index_path(segment);
    let temporary = path.with_extension("rqidx.tmp");
    let mut file = File::create(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, &path)?;
    File::open(path.parent().expect("index has parent"))?.sync_all()?;
    Ok(())
}

pub(super) fn remove(segment: &Path) -> Result<(), StorageError> {
    match fs::remove_file(index_path(segment)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn parse(segment: &Path, bytes: &[u8]) -> io::Result<RecoveryIndex> {
    if bytes.len() < HEADER_LEN || &bytes[0..4] != MAGIC || bytes[4] != VERSION {
        return Err(invalid("invalid recovery index header"));
    }
    let segment_len = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
    let segment_crc32c = u32::from_be_bytes(bytes[16..20].try_into().unwrap());
    let location_count = u32::from_be_bytes(bytes[20..24].try_into().unwrap()) as usize;
    let metadata_len = u32::from_be_bytes(bytes[24..28].try_into().unwrap()) as usize;
    let locations_len = location_count
        .checked_mul(LOCATION_LEN)
        .ok_or_else(|| invalid("recovery index location length overflow"))?;
    let expected_len = HEADER_LEN
        .checked_add(locations_len)
        .and_then(|len| len.checked_add(metadata_len))
        .ok_or_else(|| invalid("recovery index length overflow"))?;
    if expected_len != bytes.len() {
        return Err(invalid("recovery index length mismatch"));
    }
    let expected_crc = u32::from_be_bytes(bytes[28..32].try_into().unwrap());
    if crc32c::crc32c(&bytes[HEADER_LEN..]) != expected_crc {
        return Err(invalid("recovery index checksum mismatch"));
    }

    let segment = Arc::new(segment.to_path_buf());
    let mut locations = Vec::with_capacity(location_count);
    let mut cursor = HEADER_LEN;
    for _ in 0..location_count {
        let index = u64::from_be_bytes(bytes[cursor..cursor + 8].try_into().unwrap());
        let offset = u64::from_be_bytes(bytes[cursor + 8..cursor + 16].try_into().unwrap());
        let encoded_len = u64::from_be_bytes(bytes[cursor + 16..cursor + 24].try_into().unwrap());
        let end = offset
            .checked_add(encoded_len)
            .ok_or_else(|| invalid("recovery index record boundary overflow"))?;
        if encoded_len == 0 || end > segment_len {
            return Err(invalid("recovery index record boundary is invalid"));
        }
        locations.push(RecordLocation {
            index,
            segment: Arc::clone(&segment),
            offset,
            encoded_len,
        });
        cursor += LOCATION_LEN;
    }
    Ok(RecoveryIndex {
        segment_len,
        segment_crc32c,
        locations,
        metadata: bytes[cursor..].to_vec(),
    })
}

fn index_path(segment: &Path) -> PathBuf {
    segment.with_extension("rqidx")
}

fn invalid(reason: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, reason)
}
