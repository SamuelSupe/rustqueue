use crate::model::MessageMeta;
use crate::BrokerError;
use bytes::Bytes;
use rustqueue_storage::{PayloadRef, Record, RecordLocation, HEADER_LEN};
use std::sync::Arc;

const ITEM_HEADER: usize = 20;

pub(crate) struct EncodedBatch<'a> {
    count: [u8; 4],
    bodies: &'a [Bytes],
    pub entries: Vec<EncodedEntry>,
}

pub(crate) struct EncodedEntry {
    header: [u8; ITEM_HEADER],
    pub position: u64,
    pub id: u64,
    pub body_offset: usize,
    pub len: u32,
    pub crc32c: u32,
}

pub(crate) fn encode(
    first_position: u64,
    first_id: u64,
    bodies: &[Bytes],
) -> Result<EncodedBatch<'_>, BrokerError> {
    let mut entries = Vec::with_capacity(bodies.len());
    let mut payload_offset = 4usize;
    for (ordinal, body) in bodies.iter().enumerate() {
        let len = u32::try_from(body.len()).map_err(|_| BrokerError::MessageTooLarge)?;
        let position = first_position.saturating_add(ordinal as u64);
        let id = first_id.saturating_add(ordinal as u64);
        let mut header = [0u8; ITEM_HEADER];
        header[0..8].copy_from_slice(&position.to_be_bytes());
        header[8..16].copy_from_slice(&id.to_be_bytes());
        header[16..20].copy_from_slice(&len.to_be_bytes());
        let body_offset = payload_offset.saturating_add(ITEM_HEADER);
        payload_offset = body_offset.saturating_add(body.len());
        entries.push(EncodedEntry {
            header,
            position,
            id,
            body_offset,
            len,
            crc32c: crc32c::crc32c(body),
        });
    }
    Ok(EncodedBatch {
        count: (bodies.len() as u32).to_be_bytes(),
        bodies,
        entries,
    })
}

impl EncodedBatch<'_> {
    pub fn parts(&self) -> Vec<&[u8]> {
        let mut parts = Vec::with_capacity(1 + self.entries.len() * 2);
        parts.push(self.count.as_slice());
        for (entry, body) in self.entries.iter().zip(self.bodies) {
            parts.push(entry.header.as_slice());
            parts.push(body.as_ref());
        }
        parts
    }
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
    batch: &EncodedBatch<'_>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoded_batch_borrows_message_bodies() {
        let body = Bytes::from(vec![0x5a; 1024]);
        let batch = encode(1, 2, std::slice::from_ref(&body)).unwrap();
        let parts = batch.parts();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[2].as_ptr(), body.as_ptr());
        assert_eq!(parts[2].len(), body.len());
    }
}
