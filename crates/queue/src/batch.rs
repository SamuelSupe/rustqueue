use crate::model::MessageMeta;
use crate::BrokerError;
use rustqueue_storage::{PayloadRef, Record, RecordLocation, HEADER_LEN};
use std::sync::Arc;

const ITEM_HEADER: usize = 20;

pub(crate) struct EncodedBatch {
    pub payload: Vec<u8>,
    pub entries: Vec<EncodedEntry>,
}

pub(crate) struct EncodedEntry {
    pub position: u64,
    pub id: u64,
    pub body_offset: usize,
    pub len: u32,
    pub crc32c: u32,
}

pub(crate) fn encode<B: AsRef<[u8]>>(
    first_position: u64,
    first_id: u64,
    bodies: &[B],
) -> Result<EncodedBatch, BrokerError> {
    let capacity = 4usize.saturating_add(
        bodies
            .iter()
            .map(|body| ITEM_HEADER + body.as_ref().len())
            .sum::<usize>(),
    );
    let mut payload = Vec::with_capacity(capacity);
    payload.extend_from_slice(&(bodies.len() as u32).to_be_bytes());
    let mut entries = Vec::with_capacity(bodies.len());
    for (ordinal, body) in bodies.iter().enumerate() {
        let body = body.as_ref();
        let len = u32::try_from(body.len()).map_err(|_| BrokerError::MessageTooLarge)?;
        let position = first_position.saturating_add(ordinal as u64);
        let id = first_id.saturating_add(ordinal as u64);
        payload.extend_from_slice(&position.to_be_bytes());
        payload.extend_from_slice(&id.to_be_bytes());
        payload.extend_from_slice(&len.to_be_bytes());
        let body_offset = payload.len();
        payload.extend_from_slice(body);
        entries.push(EncodedEntry {
            position,
            id,
            body_offset,
            len,
            crc32c: crc32c::crc32c(body),
        });
    }
    Ok(EncodedBatch { payload, entries })
}

pub(crate) fn metas(
    record: &Record,
    location: &RecordLocation,
) -> Result<Vec<MessageMeta>, BrokerError> {
    if record.payload.len() < 4 {
        return Err(BrokerError::InvalidRecord(
            "publish batch is truncated".into(),
        ));
    }
    let count = u32::from_be_bytes(record.payload[0..4].try_into().unwrap()) as usize;
    if count == 0 {
        return Err(BrokerError::InvalidRecord("publish batch is empty".into()));
    }
    let mut cursor = 4usize;
    let mut output = Vec::with_capacity(count);
    for ordinal in 0..count {
        if cursor.saturating_add(ITEM_HEADER) > record.payload.len() {
            return Err(BrokerError::InvalidRecord(
                "publish batch item is truncated".into(),
            ));
        }
        let position = u64::from_be_bytes(record.payload[cursor..cursor + 8].try_into().unwrap());
        let id = u64::from_be_bytes(record.payload[cursor + 8..cursor + 16].try_into().unwrap());
        let len = u32::from_be_bytes(record.payload[cursor + 16..cursor + 20].try_into().unwrap());
        cursor += ITEM_HEADER;
        let end = cursor
            .checked_add(len as usize)
            .ok_or_else(|| BrokerError::InvalidRecord("publish batch length overflow".into()))?;
        if end > record.payload.len() {
            return Err(BrokerError::InvalidRecord(
                "publish batch body is truncated".into(),
            ));
        }
        if ordinal == 0 && id != record.message_id {
            return Err(BrokerError::InvalidRecord(
                "publish batch first ID mismatch".into(),
            ));
        }
        let body = &record.payload[cursor..end];
        output.push(MessageMeta {
            position,
            id,
            timestamp_ns: record.timestamp_ns,
            available_at_ms: record.available_at_ms,
            log_index: location.index,
            payload: PayloadRef {
                path: Arc::clone(&location.segment),
                offset: location.offset + HEADER_LEN as u64 + cursor as u64,
                len,
                crc32c: crc32c::crc32c(body),
            },
        });
        cursor = end;
    }
    if cursor != record.payload.len() {
        return Err(BrokerError::InvalidRecord(
            "publish batch has trailing bytes".into(),
        ));
    }
    Ok(output)
}

pub(crate) fn metas_after_append(
    timestamp_ns: i64,
    available_at_ms: i64,
    location: &RecordLocation,
    batch: &EncodedBatch,
) -> Vec<MessageMeta> {
    batch
        .entries
        .iter()
        .map(|entry| MessageMeta {
            position: entry.position,
            id: entry.id,
            timestamp_ns,
            available_at_ms,
            log_index: location.index,
            payload: PayloadRef {
                path: Arc::clone(&location.segment),
                offset: location.offset + HEADER_LEN as u64 + entry.body_offset as u64,
                len: entry.len,
                crc32c: entry.crc32c,
            },
        })
        .collect()
}
