use super::{RecordLocation, StorageError};
use std::fs::{self, File};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const MAGIC: &[u8; 4] = b"RQI7";
const VERSION: u8 = 2;
pub(super) const HEADER_LEN: u64 = 64;
const LOCATION_LEN: u64 = 24;
const MAX_INDEX_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(super) struct RecoveryIndexSummary {
    pub segment_len: u64,
    pub segment_crc32c: u32,
    pub body_crc32c: u32,
    pub location_count: u64,
    pub first_index: u64,
    pub last_index: u64,
    pub metadata_offset: u64,
    pub metadata_len: u64,
}

pub(super) fn load_summary(segment: &Path) -> Result<Option<RecoveryIndexSummary>, StorageError> {
    let path = index_path(segment);
    let mut file = match File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let file_len = file.metadata()?.len();
    if !(HEADER_LEN..=MAX_INDEX_BYTES).contains(&file_len) {
        return Err(invalid("recovery index size is invalid").into());
    }

    let mut header = [0u8; HEADER_LEN as usize];
    file.read_exact(&mut header)?;
    if &header[0..4] != MAGIC || header[4] != VERSION {
        return Err(invalid("invalid recovery index header").into());
    }
    let segment_len = u64::from_be_bytes(header[8..16].try_into().unwrap());
    let segment_crc32c = u32::from_be_bytes(header[16..20].try_into().unwrap());
    let location_count = u32::from_be_bytes(header[20..24].try_into().unwrap()) as u64;
    let metadata_len = u32::from_be_bytes(header[24..28].try_into().unwrap()) as u64;
    let body_crc32c = u32::from_be_bytes(header[28..32].try_into().unwrap());
    let first_index = u64::from_be_bytes(header[32..40].try_into().unwrap());
    let last_index = u64::from_be_bytes(header[40..48].try_into().unwrap());
    let expected_body_len = u64::from_be_bytes(header[48..56].try_into().unwrap());
    let expected_header_crc = u32::from_be_bytes(header[56..60].try_into().unwrap());
    if crc32c::crc32c(&header[..56]) != expected_header_crc {
        return Err(invalid("recovery index header checksum mismatch").into());
    }
    let locations_len = location_count
        .checked_mul(LOCATION_LEN)
        .ok_or_else(|| invalid("recovery index location length overflow"))?;
    let metadata_offset = HEADER_LEN
        .checked_add(locations_len)
        .ok_or_else(|| invalid("recovery index length overflow"))?;
    let expected_len = metadata_offset
        .checked_add(metadata_len)
        .ok_or_else(|| invalid("recovery index length overflow"))?;
    if expected_len != file_len {
        return Err(invalid("recovery index length mismatch").into());
    }
    if expected_body_len != file_len - HEADER_LEN
        || (location_count == 0 && (first_index != 0 || last_index != 0))
        || (location_count > 0 && last_index != first_index.saturating_add(location_count - 1))
    {
        return Err(invalid("recovery index range is invalid").into());
    }

    Ok(Some(RecoveryIndexSummary {
        segment_len,
        segment_crc32c,
        body_crc32c,
        location_count,
        first_index,
        last_index,
        metadata_offset,
        metadata_len,
    }))
}

pub(super) fn load_locations(
    segment: &Path,
    summary: &RecoveryIndexSummary,
) -> Result<Vec<RecordLocation>, StorageError> {
    let mut file = File::open(index_path(segment))?;
    file.seek(SeekFrom::Start(HEADER_LEN))?;
    let segment = Arc::new(segment.to_path_buf());
    let capacity = usize::try_from(summary.location_count)
        .map_err(|_| invalid("recovery index location count exceeds address space"))?;
    let mut locations = Vec::with_capacity(capacity);
    let mut bytes = [0u8; LOCATION_LEN as usize];
    for _ in 0..summary.location_count {
        file.read_exact(&mut bytes)?;
        let index = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let offset = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
        let encoded_len = u64::from_be_bytes(bytes[16..24].try_into().unwrap());
        let expected = summary.first_index + locations.len() as u64;
        if index != expected
            || encoded_len == 0
            || offset
                .checked_add(encoded_len)
                .is_none_or(|end| end > summary.segment_len)
        {
            return Err(invalid("recovery index location is invalid").into());
        }
        locations.push(RecordLocation {
            index,
            segment: Arc::clone(&segment),
            offset,
            encoded_len,
        });
    }
    Ok(locations)
}

pub(super) fn load_location(
    segment: &Path,
    summary: &RecoveryIndexSummary,
    index: u64,
) -> Result<Option<RecordLocation>, StorageError> {
    if summary.location_count == 0 || index < summary.first_index || index > summary.last_index {
        return Ok(None);
    }
    let ordinal = index - summary.first_index;
    let offset = HEADER_LEN
        .checked_add(ordinal.saturating_mul(LOCATION_LEN))
        .ok_or_else(|| invalid("recovery index location offset overflow"))?;
    let mut file = File::open(index_path(segment))?;
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = [0u8; LOCATION_LEN as usize];
    file.read_exact(&mut bytes)?;
    let actual = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
    let record_offset = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
    let encoded_len = u64::from_be_bytes(bytes[16..24].try_into().unwrap());
    if actual != index
        || encoded_len == 0
        || record_offset
            .checked_add(encoded_len)
            .is_none_or(|end| end > summary.segment_len)
    {
        return Err(invalid("recovery index location mismatch").into());
    }
    Ok(Some(RecordLocation {
        index,
        segment: Arc::new(segment.to_path_buf()),
        offset: record_offset,
        encoded_len,
    }))
}

pub(super) fn load_metadata(
    segment: &Path,
    summary: &RecoveryIndexSummary,
) -> Result<Vec<u8>, StorageError> {
    let len = usize::try_from(summary.metadata_len)
        .map_err(|_| invalid("recovery metadata exceeds address space"))?;
    let mut bytes = vec![0; len];
    let mut file = File::open(index_path(segment))?;
    file.seek(SeekFrom::Start(summary.metadata_offset))?;
    file.read_exact(&mut bytes)?;
    Ok(bytes)
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
        .checked_mul(LOCATION_LEN as usize)
        .and_then(|len| len.checked_add(metadata.len()))
        .ok_or_else(|| invalid("recovery index length overflow"))?;
    let mut body_crc32c = 0u32;
    for location in locations {
        body_crc32c = crc32c::crc32c_append(body_crc32c, &location.index.to_be_bytes());
        body_crc32c = crc32c::crc32c_append(body_crc32c, &location.offset.to_be_bytes());
        body_crc32c = crc32c::crc32c_append(body_crc32c, &location.encoded_len.to_be_bytes());
    }
    body_crc32c = crc32c::crc32c_append(body_crc32c, metadata);

    let mut header = [0u8; HEADER_LEN as usize];
    header[0..4].copy_from_slice(MAGIC);
    header[4] = VERSION;
    header[8..16].copy_from_slice(&segment_len.to_be_bytes());
    header[16..20].copy_from_slice(&segment_crc32c.to_be_bytes());
    header[20..24].copy_from_slice(&location_count.to_be_bytes());
    header[24..28].copy_from_slice(&metadata_len.to_be_bytes());
    header[28..32].copy_from_slice(&body_crc32c.to_be_bytes());
    let first_index = locations.first().map_or(0, |location| location.index);
    let last_index = locations.last().map_or(0, |location| location.index);
    header[32..40].copy_from_slice(&first_index.to_be_bytes());
    header[40..48].copy_from_slice(&last_index.to_be_bytes());
    header[48..56].copy_from_slice(&(body_len as u64).to_be_bytes());
    let header_checksum = crc32c::crc32c(&header[..56]);
    header[56..60].copy_from_slice(&header_checksum.to_be_bytes());

    let path = index_path(segment);
    let temporary = path.with_extension("rqidx.tmp");
    let mut file = BufWriter::with_capacity(64 * 1024, File::create(&temporary)?);
    file.write_all(&header)?;
    for location in locations {
        file.write_all(&location.index.to_be_bytes())?;
        file.write_all(&location.offset.to_be_bytes())?;
        file.write_all(&location.encoded_len.to_be_bytes())?;
    }
    file.write_all(metadata)?;
    file.flush()?;
    let file = file
        .into_inner()
        .map_err(|error| StorageError::Io(error.into_error()))?;
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

pub(super) fn index_path(segment: &Path) -> PathBuf {
    segment.with_extension("rqidx")
}

fn invalid(reason: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, reason)
}
