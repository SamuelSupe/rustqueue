use crate::model::MessageMeta;
use crate::BrokerError;
use rustqueue_storage::PayloadRef;
use std::path::Path;
use std::sync::Arc;

const MAGIC: &[u8; 4] = b"RQTM";
const VERSION: u32 = 1;
const HEADER_LEN: usize = 12;
const MESSAGE_LEN: usize = 56;

pub(super) fn encode<'a>(messages: impl Iterator<Item = &'a MessageMeta>) -> Vec<u8> {
    let messages: Vec<_> = messages.collect();
    let mut bytes = Vec::with_capacity(HEADER_LEN + messages.len() * MESSAGE_LEN);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&VERSION.to_be_bytes());
    bytes.extend_from_slice(&(messages.len() as u32).to_be_bytes());
    for message in messages {
        bytes.extend_from_slice(&message.position.to_be_bytes());
        bytes.extend_from_slice(&message.id.to_be_bytes());
        bytes.extend_from_slice(&message.timestamp_ns.to_be_bytes());
        bytes.extend_from_slice(&message.available_at_ms.to_be_bytes());
        bytes.extend_from_slice(&message.log_index.to_be_bytes());
        bytes.extend_from_slice(&message.payload.offset.to_be_bytes());
        bytes.extend_from_slice(&message.payload.len.to_be_bytes());
        bytes.extend_from_slice(&message.payload.crc32c.to_be_bytes());
    }
    bytes
}

pub(super) fn decode(
    path: &Path,
    segment_len: u64,
    bytes: &[u8],
) -> Result<Vec<MessageMeta>, BrokerError> {
    if bytes.len() < HEADER_LEN
        || &bytes[0..4] != MAGIC
        || u32::from_be_bytes(bytes[4..8].try_into().unwrap()) != VERSION
    {
        return Err(BrokerError::InvalidRecord(
            "topic recovery index header is invalid".into(),
        ));
    }
    let count = u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let expected = count
        .checked_mul(MESSAGE_LEN)
        .and_then(|len| len.checked_add(HEADER_LEN))
        .ok_or_else(|| BrokerError::InvalidRecord("topic recovery index overflow".into()))?;
    if bytes.len() != expected {
        return Err(BrokerError::InvalidRecord(
            "topic recovery index length mismatch".into(),
        ));
    }

    let path = Arc::new(path.to_path_buf());
    let mut messages = Vec::with_capacity(count);
    let mut cursor = HEADER_LEN;
    for _ in 0..count {
        let position = u64::from_be_bytes(bytes[cursor..cursor + 8].try_into().unwrap());
        let id = u64::from_be_bytes(bytes[cursor + 8..cursor + 16].try_into().unwrap());
        let timestamp_ns = i64::from_be_bytes(bytes[cursor + 16..cursor + 24].try_into().unwrap());
        let available_at_ms =
            i64::from_be_bytes(bytes[cursor + 24..cursor + 32].try_into().unwrap());
        let log_index = u64::from_be_bytes(bytes[cursor + 32..cursor + 40].try_into().unwrap());
        let offset = u64::from_be_bytes(bytes[cursor + 40..cursor + 48].try_into().unwrap());
        let len = u32::from_be_bytes(bytes[cursor + 48..cursor + 52].try_into().unwrap());
        let crc32c = u32::from_be_bytes(bytes[cursor + 52..cursor + 56].try_into().unwrap());
        if len == 0
            || offset
                .checked_add(len as u64)
                .is_none_or(|end| end > segment_len)
        {
            return Err(BrokerError::InvalidRecord(
                "topic recovery payload boundary is invalid".into(),
            ));
        }
        messages.push(MessageMeta {
            position,
            id,
            timestamp_ns,
            available_at_ms,
            log_index,
            payload: PayloadRef {
                path: Arc::clone(&path),
                offset,
                len,
                crc32c,
            },
        });
        cursor += MESSAGE_LEN;
    }
    Ok(messages)
}
