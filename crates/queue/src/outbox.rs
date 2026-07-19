use crate::BrokerError;
use bytes::Bytes;
use rustqueue_storage::MAX_RECORD_BYTES;
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const MAGIC: &[u8; 4] = b"RQO7";
const HEADER_LEN: usize = 28;
const MAX_OUTBOX_BYTES: u64 = MAX_RECORD_BYTES as u64 + 3 * u16::MAX as u64 + HEADER_LEN as u64;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
pub(crate) struct OutboxEntry {
    pub source_topic: String,
    pub source_channel: String,
    pub message_id: u64,
    pub target_topic: String,
    pub body: Bytes,
}

#[derive(Clone, Copy)]
struct Header {
    source_topic_len: usize,
    source_channel_len: usize,
    target_topic_len: usize,
    message_id: u64,
    body_len: usize,
}

pub(crate) fn store(directory: &Path, entry: &OutboxEntry) -> Result<PathBuf, BrokerError> {
    fs::create_dir_all(directory)?;
    let source_key = source_key(&entry.source_topic, &entry.source_channel);
    let stem = format!("{:016x}-{source_key}", entry.message_id);
    let path = directory.join(format!("{stem}.outbox"));
    let temporary = directory.join(format!(
        "{stem}.{}-{}.tmp",
        std::process::id(),
        TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let source_topic = entry.source_topic.as_bytes();
    let source_channel = entry.source_channel.as_bytes();
    let target_topic = entry.target_topic.as_bytes();
    let source_topic_len = u16::try_from(source_topic.len())
        .map_err(|_| BrokerError::InvalidRecord("DLQ source topic is too long".into()))?;
    let source_channel_len = u16::try_from(source_channel.len())
        .map_err(|_| BrokerError::InvalidRecord("DLQ source channel is too long".into()))?;
    let target_topic_len = u16::try_from(target_topic.len())
        .map_err(|_| BrokerError::InvalidRecord("DLQ target topic is too long".into()))?;
    let body_len = u32::try_from(entry.body.len())
        .map_err(|_| BrokerError::InvalidRecord("DLQ body is too large".into()))?;
    let encoded_len = HEADER_LEN
        .checked_add(source_topic.len())
        .and_then(|len| len.checked_add(source_channel.len()))
        .and_then(|len| len.checked_add(target_topic.len()))
        .and_then(|len| len.checked_add(entry.body.len()))
        .ok_or_else(|| BrokerError::InvalidRecord("DLQ outbox length overflow".into()))?;
    if encoded_len as u64 > MAX_OUTBOX_BYTES {
        return Err(BrokerError::InvalidRecord(
            "DLQ outbox file exceeds the maximum record size".into(),
        ));
    }
    let mut header = [0u8; HEADER_LEN];
    header[0..4].copy_from_slice(MAGIC);
    header[4..6].copy_from_slice(&source_topic_len.to_be_bytes());
    header[6..8].copy_from_slice(&source_channel_len.to_be_bytes());
    header[8..10].copy_from_slice(&target_topic_len.to_be_bytes());
    header[12..20].copy_from_slice(&entry.message_id.to_be_bytes());
    header[20..24].copy_from_slice(&body_len.to_be_bytes());
    let mut checksum = crc32c::crc32c(&header[..24]);
    for bytes in [source_topic, source_channel, target_topic, &entry.body] {
        checksum = crc32c::crc32c_append(checksum, bytes);
    }
    header[24..28].copy_from_slice(&checksum.to_be_bytes());
    let mut file = File::create(&temporary)?;
    file.write_all(&header)?;
    file.write_all(source_topic)?;
    file.write_all(source_channel)?;
    file.write_all(target_topic)?;
    file.write_all(&entry.body)?;
    file.sync_all()?;
    fs::rename(&temporary, &path)?;
    File::open(directory)?.sync_all()?;
    Ok(path)
}

pub(crate) fn paths(directory: &Path) -> Result<Vec<PathBuf>, BrokerError> {
    if !directory.exists() {
        return Ok(Vec::new());
    }
    let mut paths = Vec::new();
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if path
            .extension()
            .is_some_and(|extension| extension == "outbox")
        {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

pub(crate) fn load(path: &Path) -> Result<OutboxEntry, BrokerError> {
    if fs::metadata(path)?.len() > MAX_OUTBOX_BYTES {
        return Err(BrokerError::InvalidRecord(
            "DLQ outbox file exceeds the maximum record size".into(),
        ));
    }
    let bytes = fs::read(path)?;
    let header = parse_header(&bytes)?;
    validate_layout(header, bytes.len() as u64)?;
    let expected = u32::from_be_bytes(bytes[24..28].try_into().unwrap());
    let actual = crc32c::crc32c_append(crc32c::crc32c(&bytes[..24]), &bytes[HEADER_LEN..]);
    if actual != expected {
        return Err(BrokerError::InvalidRecord(
            "DLQ outbox checksum is invalid".into(),
        ));
    }
    let mut cursor = HEADER_LEN;
    let source_topic = read_string(&bytes, &mut cursor, header.source_topic_len)?;
    let source_channel = read_string(&bytes, &mut cursor, header.source_channel_len)?;
    let target_topic = read_string(&bytes, &mut cursor, header.target_topic_len)?;
    validate_path(path, header.message_id, &source_topic, &source_channel)?;
    let body = Bytes::from(bytes).slice(cursor..);
    Ok(OutboxEntry {
        source_topic,
        source_channel,
        message_id: header.message_id,
        target_topic,
        body,
    })
}

/// Returns only the source references needed by GC. The body is intentionally
/// not read: keeping the source segment is conservative even if later full
/// outbox verification fails during recovery.
pub(crate) fn retained_sources(directory: &Path) -> Result<Vec<(String, u64)>, BrokerError> {
    paths(directory)?
        .into_iter()
        .map(|path| {
            let mut file = File::open(&path)?;
            let file_len = file.metadata()?.len();
            if file_len > MAX_OUTBOX_BYTES {
                return Err(BrokerError::InvalidRecord(
                    "DLQ outbox file exceeds the maximum record size".into(),
                ));
            }
            let mut bytes = [0u8; HEADER_LEN];
            file.read_exact(&mut bytes)?;
            let header = parse_header(&bytes)?;
            validate_layout(header, file_len)?;
            let names_len = header
                .source_topic_len
                .checked_add(header.source_channel_len)
                .and_then(|len| len.checked_add(header.target_topic_len))
                .ok_or_else(|| {
                    BrokerError::InvalidRecord("DLQ outbox name length overflow".into())
                })?;
            let mut names = vec![0u8; names_len];
            file.read_exact(&mut names)?;
            let mut cursor = 0;
            let source_topic = read_string(&names, &mut cursor, header.source_topic_len)?;
            let source_channel = read_string(&names, &mut cursor, header.source_channel_len)?;
            validate_path(&path, header.message_id, &source_topic, &source_channel)?;
            Ok((source_topic, header.message_id))
        })
        .collect()
}

pub(crate) fn cleanup_temporary(directory: &Path) -> Result<(), BrokerError> {
    if !directory.exists() {
        return Ok(());
    }
    let mut changed = false;
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if path.extension().is_some_and(|extension| extension == "tmp") {
            fs::remove_file(path)?;
            changed = true;
        }
    }
    if changed {
        File::open(directory)?.sync_all()?;
    }
    Ok(())
}

fn parse_header(bytes: &[u8]) -> Result<Header, BrokerError> {
    if bytes.len() < HEADER_LEN || &bytes[0..4] != MAGIC || bytes[10..12] != [0, 0] {
        return Err(BrokerError::InvalidRecord(
            "DLQ outbox header is invalid".into(),
        ));
    }
    Ok(Header {
        source_topic_len: u16::from_be_bytes(bytes[4..6].try_into().unwrap()) as usize,
        source_channel_len: u16::from_be_bytes(bytes[6..8].try_into().unwrap()) as usize,
        target_topic_len: u16::from_be_bytes(bytes[8..10].try_into().unwrap()) as usize,
        message_id: u64::from_be_bytes(bytes[12..20].try_into().unwrap()),
        body_len: u32::from_be_bytes(bytes[20..24].try_into().unwrap()) as usize,
    })
}

fn validate_layout(header: Header, file_len: u64) -> Result<(), BrokerError> {
    let payload_len = header
        .source_topic_len
        .checked_add(header.source_channel_len)
        .and_then(|len| len.checked_add(header.target_topic_len))
        .and_then(|len| len.checked_add(header.body_len))
        .ok_or_else(|| BrokerError::InvalidRecord("DLQ outbox length overflow".into()))?;
    if HEADER_LEN
        .checked_add(payload_len)
        .is_none_or(|expected| expected as u64 != file_len)
    {
        return Err(BrokerError::InvalidRecord(
            "DLQ outbox length is invalid".into(),
        ));
    }
    Ok(())
}

fn source_key(topic: &str, channel: &str) -> String {
    let mut digest = Sha256::new();
    for value in [topic.as_bytes(), channel.as_bytes()] {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value);
    }
    hex::encode(&digest.finalize()[..16])
}

fn legacy_source_key(topic: &str, channel: &str) -> u32 {
    let mut checksum = crc32c::crc32c(topic.as_bytes());
    checksum = crc32c::crc32c_append(checksum, &[0]);
    crc32c::crc32c_append(checksum, channel.as_bytes())
}

fn validate_path(
    path: &Path,
    message_id: u64,
    source_topic: &str,
    source_channel: &str,
) -> Result<(), BrokerError> {
    let stem = path.file_stem().and_then(|value| value.to_str());
    let modern = format!(
        "{message_id:016x}-{}",
        source_key(source_topic, source_channel)
    );
    let legacy = format!(
        "{message_id:016x}-{:08x}",
        legacy_source_key(source_topic, source_channel)
    );
    if stem != Some(modern.as_str()) && stem != Some(legacy.as_str()) {
        return Err(BrokerError::InvalidRecord(
            "DLQ outbox filename does not match its source".into(),
        ));
    }
    Ok(())
}

fn read_string(bytes: &[u8], cursor: &mut usize, len: usize) -> Result<String, BrokerError> {
    let end = cursor
        .checked_add(len)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| BrokerError::InvalidRecord("DLQ outbox string is truncated".into()))?;
    let value = std::str::from_utf8(&bytes[*cursor..end])
        .map_err(|_| BrokerError::InvalidRecord("DLQ outbox string is not UTF-8".into()))?
        .to_owned();
    *cursor = end;
    Ok(value)
}

pub(crate) fn remove(path: &Path) -> Result<(), BrokerError> {
    let parent = path.parent().expect("outbox has parent");
    match fs::remove_file(path) {
        Ok(()) => File::open(parent)?.sync_all()?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn outbox_round_trips_without_json_body_amplification() {
        let directory = tempdir().unwrap();
        let entry = OutboxEntry {
            source_topic: "events".into(),
            source_channel: "workers".into(),
            message_id: 42,
            target_topic: "events.DLQ".into(),
            body: Bytes::from(vec![0xab; 1024 * 1024]),
        };
        let path = store(directory.path(), &entry).unwrap();
        assert!(std::fs::metadata(&path).unwrap().len() < 1024 * 1024 + 1024);
        let loaded = load(&path).unwrap();
        assert_eq!(loaded.message_id, entry.message_id);
        assert_eq!(loaded.body, entry.body);
    }

    #[test]
    fn fan_out_channels_with_the_same_message_id_use_distinct_entries() {
        let directory = tempdir().unwrap();
        let entry = |channel: &str| OutboxEntry {
            source_topic: "events".into(),
            source_channel: channel.into(),
            message_id: 42,
            target_topic: format!("events.{channel}.DLQ"),
            body: Bytes::from_static(b"payload"),
        };
        let first = store(directory.path(), &entry("alpha")).unwrap();
        let second = store(directory.path(), &entry("beta")).unwrap();
        assert_ne!(first, second);
        let loaded: Vec<_> = paths(directory.path())
            .unwrap()
            .iter()
            .map(|path| load(path).unwrap())
            .collect();
        assert_eq!(loaded.len(), 2);
        assert_ne!(loaded[0].source_channel, loaded[1].source_channel);
    }

    #[test]
    fn gc_metadata_scan_does_not_load_large_message_bodies() {
        let directory = tempdir().unwrap();
        let entry = OutboxEntry {
            source_topic: "events".into(),
            source_channel: "workers".into(),
            message_id: 42,
            target_topic: "events.DLQ".into(),
            body: Bytes::from(vec![0xab; 8 * 1024 * 1024]),
        };
        store(directory.path(), &entry).unwrap();
        assert_eq!(
            retained_sources(directory.path()).unwrap(),
            vec![("events".into(), 42)]
        );
    }

    #[test]
    fn outbox_limit_covers_the_full_storage_record_contract() {
        assert!(
            MAX_OUTBOX_BYTES >= MAX_RECORD_BYTES as u64 + 3 * u16::MAX as u64 + HEADER_LEN as u64
        );
    }
}
