use crate::model::MessageMeta;
use crate::BrokerError;
use rustqueue_storage::{PayloadRef, RecoveryMetadataRef};
use std::path::PathBuf;
use std::sync::Arc;

const MAGIC: &[u8; 4] = b"RQTM";
const VERSION: u32 = 2;
pub(super) const HEADER_LEN: usize = 12;
pub(super) const MESSAGE_LEN: usize = 60;

#[derive(Clone, Debug)]
pub(super) struct Summary {
    pub count: u64,
    pub first: MessageMeta,
    pub last: MessageMeta,
    pub scheduled: Vec<(i64, u64)>,
}

pub(super) fn encode<'a>(messages: impl Iterator<Item = &'a MessageMeta> + Clone) -> Vec<u8> {
    let count = messages.clone().count();
    let mut bytes = Vec::with_capacity(HEADER_LEN + count * MESSAGE_LEN);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&VERSION.to_be_bytes());
    bytes.extend_from_slice(&(count as u32).to_be_bytes());
    for message in messages {
        let start = bytes.len();
        bytes.extend_from_slice(&message.position.to_be_bytes());
        bytes.extend_from_slice(&message.id.to_be_bytes());
        bytes.extend_from_slice(&message.timestamp_ns.to_be_bytes());
        bytes.extend_from_slice(&message.available_at_ms.to_be_bytes());
        bytes.extend_from_slice(&message.log_index.to_be_bytes());
        bytes.extend_from_slice(&message.payload.offset.to_be_bytes());
        bytes.extend_from_slice(&message.payload.len.to_be_bytes());
        bytes.extend_from_slice(&message.payload.crc32c.to_be_bytes());
        bytes.extend_from_slice(&crc32c::crc32c(&bytes[start..]).to_be_bytes());
    }
    bytes
}

pub(super) fn inspect(
    reference: &RecoveryMetadataRef,
    now_ms: i64,
) -> Result<Summary, BrokerError> {
    let header = reference.read_range(0, HEADER_LEN)?;
    let count = decode_header(&header)?;
    if count == 0 {
        return Err(BrokerError::InvalidRecord(
            "topic recovery index is empty".into(),
        ));
    }
    let expected = HEADER_LEN as u64
        + count
            .checked_mul(MESSAGE_LEN as u64)
            .ok_or_else(|| BrokerError::InvalidRecord("topic recovery index overflow".into()))?;
    if reference.len() != expected {
        return Err(BrokerError::InvalidRecord(
            "topic recovery index length mismatch".into(),
        ));
    }
    let mut scheduled = Vec::new();
    let mut first = None;
    let mut last: Option<MessageMeta> = None;
    let mut ordinal = 0;
    while ordinal < count {
        let page = read_page(reference, ordinal, (count - ordinal).min(1024) as usize)?;
        for message in page {
            if last.as_ref().is_some_and(|previous| {
                message.position != previous.position.saturating_add(1) || message.id < previous.id
            }) {
                return Err(BrokerError::InvalidRecord(
                    "topic recovery index range is invalid".into(),
                ));
            }
            if message.available_at_ms > now_ms {
                scheduled.push((message.available_at_ms, message.position));
            }
            first.get_or_insert_with(|| message.clone());
            last = Some(message);
        }
        ordinal += (count - ordinal).min(1024);
    }
    Ok(Summary {
        count,
        first: first.expect("non-empty recovery index"),
        last: last.expect("non-empty recovery index"),
        scheduled,
    })
}

pub(super) fn read_page(
    reference: &RecoveryMetadataRef,
    first_ordinal: u64,
    count: usize,
) -> Result<Vec<MessageMeta>, BrokerError> {
    if count == 0 {
        return Ok(Vec::new());
    }
    let offset = HEADER_LEN as u64
        + first_ordinal
            .checked_mul(MESSAGE_LEN as u64)
            .ok_or_else(|| BrokerError::InvalidRecord("topic index offset overflow".into()))?;
    let length = count
        .checked_mul(MESSAGE_LEN)
        .ok_or_else(|| BrokerError::InvalidRecord("topic index page overflow".into()))?;
    let bytes = reference.read_range(offset, length)?;
    let mut messages = Vec::with_capacity(count);
    let path = Arc::new(reference.segment_path().to_path_buf());
    for entry in bytes.chunks_exact(MESSAGE_LEN) {
        messages.push(decode_entry(
            Arc::clone(&path),
            reference.segment_len(),
            entry,
        )?);
    }
    Ok(messages)
}

fn decode_header(bytes: &[u8]) -> Result<u64, BrokerError> {
    if bytes.len() < HEADER_LEN
        || &bytes[0..4] != MAGIC
        || u32::from_be_bytes(bytes[4..8].try_into().unwrap()) != VERSION
    {
        return Err(BrokerError::InvalidRecord(
            "topic recovery index header is invalid".into(),
        ));
    }
    Ok(u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as u64)
}

fn decode_entry(
    path: Arc<PathBuf>,
    segment_len: u64,
    bytes: &[u8],
) -> Result<MessageMeta, BrokerError> {
    if bytes.len() != MESSAGE_LEN
        || crc32c::crc32c(&bytes[..MESSAGE_LEN - 4])
            != u32::from_be_bytes(bytes[MESSAGE_LEN - 4..].try_into().unwrap())
    {
        return Err(BrokerError::InvalidRecord(
            "topic recovery entry checksum mismatch".into(),
        ));
    }
    let position = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
    let id = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
    let timestamp_ns = i64::from_be_bytes(bytes[16..24].try_into().unwrap());
    let available_at_ms = i64::from_be_bytes(bytes[24..32].try_into().unwrap());
    let log_index = u64::from_be_bytes(bytes[32..40].try_into().unwrap());
    let offset = u64::from_be_bytes(bytes[40..48].try_into().unwrap());
    let len = u32::from_be_bytes(bytes[48..52].try_into().unwrap());
    let crc32c = u32::from_be_bytes(bytes[52..56].try_into().unwrap());
    if position == 0
        || id == 0
        || len == 0
        || offset
            .checked_add(len as u64)
            .is_none_or(|end| end > segment_len)
    {
        return Err(BrokerError::InvalidRecord(
            "topic recovery payload boundary is invalid".into(),
        ));
    }
    Ok(MessageMeta {
        position,
        id,
        timestamp_ns,
        available_at_ms,
        log_index,
        payload: PayloadRef {
            path,
            offset,
            len,
            crc32c,
        },
    })
}
