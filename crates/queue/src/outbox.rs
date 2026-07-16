use crate::BrokerError;
use bytes::Bytes;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 4] = b"RQO7";
const HEADER_LEN: usize = 28;

#[derive(Clone, Debug)]
pub(crate) struct OutboxEntry {
    pub source_topic: String,
    pub source_channel: String,
    pub message_id: u64,
    pub target_topic: String,
    pub body: Bytes,
}

pub(crate) fn store(directory: &Path, entry: &OutboxEntry) -> Result<PathBuf, BrokerError> {
    fs::create_dir_all(directory)?;
    let path = directory.join(format!("{:016x}.outbox", entry.message_id));
    let temporary = directory.join(format!("{:016x}.tmp", entry.message_id));
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

pub(crate) fn load_all(directory: &Path) -> Result<Vec<(PathBuf, OutboxEntry)>, BrokerError> {
    if !directory.exists() {
        return Ok(Vec::new());
    }
    let mut paths: Vec<_> = fs::read_dir(directory)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "outbox")
        })
        .collect();
    paths.sort();
    paths
        .into_iter()
        .map(|path| {
            let bytes = fs::read(&path)?;
            if bytes.len() < HEADER_LEN || &bytes[0..4] != MAGIC {
                return Err(BrokerError::InvalidRecord(
                    "DLQ outbox header is invalid".into(),
                ));
            }
            let source_topic_len = u16::from_be_bytes(bytes[4..6].try_into().unwrap()) as usize;
            let source_channel_len = u16::from_be_bytes(bytes[6..8].try_into().unwrap()) as usize;
            let target_topic_len = u16::from_be_bytes(bytes[8..10].try_into().unwrap()) as usize;
            let message_id = u64::from_be_bytes(bytes[12..20].try_into().unwrap());
            let body_len = u32::from_be_bytes(bytes[20..24].try_into().unwrap()) as usize;
            let payload_len = source_topic_len
                .checked_add(source_channel_len)
                .and_then(|len| len.checked_add(target_topic_len))
                .and_then(|len| len.checked_add(body_len))
                .ok_or_else(|| BrokerError::InvalidRecord("DLQ outbox length overflow".into()))?;
            if payload_len != bytes.len() - HEADER_LEN {
                return Err(BrokerError::InvalidRecord(
                    "DLQ outbox length is invalid".into(),
                ));
            }
            let expected = u32::from_be_bytes(bytes[24..28].try_into().unwrap());
            let actual = crc32c::crc32c_append(crc32c::crc32c(&bytes[..24]), &bytes[HEADER_LEN..]);
            if actual != expected {
                return Err(BrokerError::InvalidRecord(
                    "DLQ outbox checksum is invalid".into(),
                ));
            }
            let mut cursor = HEADER_LEN;
            let source_topic = read_string(&bytes, &mut cursor, source_topic_len)?;
            let source_channel = read_string(&bytes, &mut cursor, source_channel_len)?;
            let target_topic = read_string(&bytes, &mut cursor, target_topic_len)?;
            let body = Bytes::copy_from_slice(&bytes[cursor..]);
            let entry = OutboxEntry {
                source_topic,
                source_channel,
                message_id,
                target_topic,
                body,
            };
            Ok((path, entry))
        })
        .collect()
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
        let loaded = load_all(directory.path()).unwrap().pop().unwrap().1;
        assert_eq!(loaded.message_id, entry.message_id);
        assert_eq!(loaded.body, entry.body);
    }
}
