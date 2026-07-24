use crate::model::MessageMeta;
use crate::BrokerError;
use bytes::Bytes;
#[cfg(test)]
use rustqueue_storage::Record;
use rustqueue_storage::{PayloadRef, RecordLocation, HEADER_LEN};
use std::io::{self, Read};
use std::sync::Arc;

const ITEM_HEADER: usize = 20;
pub(crate) const MAX_MESSAGES: usize = rustqueue_protocol::MAX_MPUB_MESSAGES;

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

#[cfg(test)]
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
    let maximum_count = record.payload.len().saturating_sub(4) / ITEM_HEADER;
    if count == 0 || count > MAX_MESSAGES || count > maximum_count {
        return Err(BrokerError::InvalidRecord(
            "publish batch count is invalid".into(),
        ));
    }
    let mut cursor = 4usize;
    let mut output = Vec::with_capacity(count);
    let mut first_position = None;
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
        let base_position = *first_position.get_or_insert(position);
        let expected_position = base_position
            .checked_add(ordinal as u64)
            .ok_or_else(|| BrokerError::InvalidRecord("publish batch position overflow".into()))?;
        let expected_id = record
            .message_id
            .checked_add(ordinal as u64)
            .ok_or_else(|| {
                BrokerError::InvalidRecord("publish batch message ID overflow".into())
            })?;
        if position != expected_position || id != expected_id {
            return Err(BrokerError::InvalidRecord(
                "publish batch positions or message IDs are not contiguous".into(),
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

pub(crate) fn metas_from_reader(
    header: rustqueue_storage::RecordHeader,
    payload_len: usize,
    reader: &mut dyn Read,
    location: &RecordLocation,
) -> io::Result<Vec<MessageMeta>> {
    if payload_len < 4 {
        return Err(invalid_data("publish batch is truncated"));
    }
    let mut count = [0u8; 4];
    reader.read_exact(&mut count)?;
    let count = u32::from_be_bytes(count) as usize;
    let maximum_count = payload_len.saturating_sub(4) / ITEM_HEADER;
    if count == 0 || count > MAX_MESSAGES || count > maximum_count {
        return Err(invalid_data("publish batch count is invalid"));
    }
    let mut cursor = 4usize;
    let mut first_position = None;
    let mut output = Vec::with_capacity(count);
    let mut body_buffer = [0u8; 64 * 1024];
    for ordinal in 0..count {
        let mut item = [0u8; ITEM_HEADER];
        reader.read_exact(&mut item)?;
        cursor = cursor.saturating_add(ITEM_HEADER);
        let position = u64::from_be_bytes(item[0..8].try_into().unwrap());
        let id = u64::from_be_bytes(item[8..16].try_into().unwrap());
        let len = u32::from_be_bytes(item[16..20].try_into().unwrap());
        let end = cursor
            .checked_add(len as usize)
            .ok_or_else(|| invalid_data("publish batch length overflow"))?;
        if end > payload_len {
            return Err(invalid_data("publish batch body is truncated"));
        }
        let base_position = *first_position.get_or_insert(position);
        let expected_position = base_position
            .checked_add(ordinal as u64)
            .ok_or_else(|| invalid_data("publish batch position overflow"))?;
        let expected_id = header
            .message_id
            .checked_add(ordinal as u64)
            .ok_or_else(|| invalid_data("publish batch message ID overflow"))?;
        if position != expected_position || id != expected_id {
            return Err(invalid_data(
                "publish batch positions or message IDs are not contiguous",
            ));
        }
        let body_offset = cursor;
        let mut remaining = len as usize;
        let mut body_crc = 0u32;
        while remaining > 0 {
            let wanted = remaining.min(body_buffer.len());
            reader.read_exact(&mut body_buffer[..wanted])?;
            body_crc = crc32c::crc32c_append(body_crc, &body_buffer[..wanted]);
            remaining -= wanted;
        }
        output.push(MessageMeta {
            position,
            id,
            timestamp_ns: header.timestamp_ns,
            available_at_ms: header.available_at_ms,
            log_index: location.index,
            payload: PayloadRef {
                path: Arc::clone(&location.segment),
                offset: location.offset + HEADER_LEN as u64 + body_offset as u64,
                len,
                crc32c: body_crc,
            },
        });
        cursor = end;
    }
    if cursor != payload_len {
        return Err(invalid_data("publish batch has trailing bytes"));
    }
    Ok(output)
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
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
    use rustqueue_storage::RecordKind;
    use std::path::PathBuf;

    #[test]
    fn encoded_batch_borrows_message_bodies() {
        let body = Bytes::from(vec![0x5a; 1024]);
        let batch = encode(1, 2, std::slice::from_ref(&body)).unwrap();
        let parts = batch.parts();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[2].as_ptr(), body.as_ptr());
        assert_eq!(parts[2].len(), body.len());
    }

    #[test]
    fn recovery_rejects_unbounded_batch_count_before_allocating() {
        let record = Record {
            kind: RecordKind::PublishBatch,
            flags: 0,
            index: 1,
            message_id: 1,
            timestamp_ns: 0,
            available_at_ms: 0,
            payload: u32::MAX.to_be_bytes().to_vec(),
        };
        let location = RecordLocation {
            index: 1,
            segment: Arc::new(PathBuf::from("segment.log")),
            offset: 0,
            encoded_len: HEADER_LEN as u64 + 4,
        };

        let error = metas(&record, &location).unwrap_err();
        assert!(matches!(error, BrokerError::InvalidRecord(_)));
    }

    #[test]
    fn recovery_rejects_non_contiguous_batch_identity() {
        let location = RecordLocation {
            index: 1,
            segment: Arc::new(PathBuf::from("segment.log")),
            offset: 0,
            encoded_len: 0,
        };
        for (second_position, second_id) in [(3u64, 11u64), (2, 12)] {
            let mut payload = 2u32.to_be_bytes().to_vec();
            for (position, id) in [(1u64, 10u64), (second_position, second_id)] {
                payload.extend_from_slice(&position.to_be_bytes());
                payload.extend_from_slice(&id.to_be_bytes());
                payload.extend_from_slice(&1u32.to_be_bytes());
                payload.push(b'x');
            }
            let record = Record {
                kind: RecordKind::PublishBatch,
                flags: 0,
                index: 1,
                message_id: 10,
                timestamp_ns: 0,
                available_at_ms: 0,
                payload,
            };
            assert!(matches!(
                metas(&record, &location),
                Err(BrokerError::InvalidRecord(_))
            ));
        }
    }
}
