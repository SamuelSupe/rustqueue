use crate::{QueueCommand, TypeConfig};
use bytes::Bytes;
use openraft::{Entry, EntryPayload};
use rustqueue_storage::{PayloadRef, RecordLocation, HEADER_LEN};
use std::io;
use std::sync::Arc;

const MAGIC: &[u8; 4] = b"RQE6";
const VERSION: u8 = 1;
const HEADER_BYTES: usize = 16;
const TABLE_ITEM_BYTES: usize = 8;
type DecodedMetadata = (Entry<TypeConfig>, Vec<(u32, u32)>, usize);

pub struct EncodedEntry {
    pub bytes: Vec<u8>,
}

pub fn encode(entry: &Entry<TypeConfig>) -> io::Result<EncodedEntry> {
    let mut metadata = entry.clone();
    let mut bodies = Vec::new();
    if let EntryPayload::Normal(envelope) = &mut metadata.payload {
        strip_bodies(&mut envelope.command, &mut bodies);
    }
    let metadata = serde_json::to_vec(&metadata).map_err(io::Error::other)?;
    let metadata_len: u32 = metadata
        .len()
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "entry metadata too large"))?;
    let body_count: u32 = bodies
        .len()
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "too many entry bodies"))?;
    let table_bytes = bodies.len().saturating_mul(TABLE_ITEM_BYTES);
    let capacity = HEADER_BYTES
        .saturating_add(metadata.len())
        .saturating_add(table_bytes)
        .saturating_add(bodies.iter().map(Bytes::len).sum::<usize>());
    let mut output = Vec::with_capacity(capacity);
    output.extend_from_slice(MAGIC);
    output.push(VERSION);
    output.extend_from_slice(&[0; 3]);
    output.extend_from_slice(&metadata_len.to_be_bytes());
    output.extend_from_slice(&body_count.to_be_bytes());
    output.extend_from_slice(&metadata);
    for body in &bodies {
        let length: u32 = body
            .len()
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "entry body too large"))?;
        output.extend_from_slice(&length.to_be_bytes());
        output.extend_from_slice(&crc32c::crc32c(body).to_be_bytes());
    }
    for body in bodies {
        output.extend_from_slice(&body);
    }
    Ok(EncodedEntry { bytes: output })
}

pub fn decode(input: &[u8]) -> io::Result<Entry<TypeConfig>> {
    let (mut entry, table, body_start) = decode_metadata(input)?;
    let mut bodies = Vec::with_capacity(table.len());
    let mut offset = body_start;
    for (length, expected_crc) in table {
        let end = offset.saturating_add(length as usize);
        if end > input.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated entry body",
            ));
        }
        let body = &input[offset..end];
        if crc32c::crc32c(body) != expected_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "entry body checksum mismatch",
            ));
        }
        bodies.push(Bytes::copy_from_slice(body));
        offset = end;
    }
    if offset != input.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "entry contains trailing bytes",
        ));
    }
    if let EntryPayload::Normal(envelope) = &mut entry.payload {
        envelope
            .validate()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let mut bodies = bodies.into_iter();
        restore_bodies(&mut envelope.command, &mut bodies)?;
        if bodies.next().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "entry contains unused bodies",
            ));
        }
    } else if !bodies.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "non-command entry contains bodies",
        ));
    }
    Ok(entry)
}

pub fn decode_log_id(input: &[u8]) -> io::Result<openraft::LogId<crate::NodeId>> {
    decode_metadata(input).map(|(entry, _, _)| entry.log_id)
}

pub fn decode_without_bodies(input: &[u8]) -> io::Result<Entry<TypeConfig>> {
    let (entry, table, body_start) = decode_metadata(input)?;
    let total = table.iter().try_fold(body_start, |offset, (length, _)| {
        offset.checked_add(*length as usize)
    });
    if total != Some(input.len()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "entry body table does not match record length",
        ));
    }
    Ok(entry)
}

pub fn payload_refs(input: &[u8], location: &RecordLocation) -> io::Result<Vec<PayloadRef>> {
    let (_, table, body_start) = decode_metadata(input)?;
    let mut offset = body_start;
    let mut references = Vec::with_capacity(table.len());
    let path = Arc::clone(&location.segment);
    for (length, crc32c) in table {
        let end = offset.saturating_add(length as usize);
        if end > input.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated entry body",
            ));
        }
        references.push(PayloadRef {
            path: Arc::clone(&path),
            offset: location.offset + HEADER_LEN as u64 + offset as u64,
            len: length,
            crc32c,
        });
        offset = end;
    }
    if offset != input.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "entry contains trailing bytes",
        ));
    }
    Ok(references)
}

fn decode_metadata(input: &[u8]) -> io::Result<DecodedMetadata> {
    if input.len() < HEADER_BYTES || &input[..4] != MAGIC || input[4] != VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid entry envelope",
        ));
    }
    let metadata_len = u32::from_be_bytes(input[8..12].try_into().unwrap()) as usize;
    let body_count = u32::from_be_bytes(input[12..16].try_into().unwrap()) as usize;
    let metadata_end = HEADER_BYTES.saturating_add(metadata_len);
    let table_end = metadata_end.saturating_add(body_count.saturating_mul(TABLE_ITEM_BYTES));
    if table_end > input.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated entry metadata or body table",
        ));
    }
    let entry: Entry<TypeConfig> = serde_json::from_slice(&input[HEADER_BYTES..metadata_end])
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if let EntryPayload::Normal(envelope) = &entry.payload {
        envelope
            .validate()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    }
    let mut table = Vec::with_capacity(body_count);
    let mut cursor = metadata_end;
    while cursor < table_end {
        let length = u32::from_be_bytes(input[cursor..cursor + 4].try_into().unwrap());
        let crc32c = u32::from_be_bytes(input[cursor + 4..cursor + 8].try_into().unwrap());
        table.push((length, crc32c));
        cursor += TABLE_ITEM_BYTES;
    }
    Ok((entry, table, table_end))
}

fn strip_bodies(command: &mut QueueCommand, output: &mut Vec<Bytes>) {
    match command {
        QueueCommand::Batch { commands } => {
            for command in commands {
                strip_bodies(command, output);
            }
        }
        QueueCommand::Publish { bodies, .. } => {
            let count = bodies.len();
            output.append(bodies);
            *bodies = vec![Bytes::new(); count];
        }
        _ => {}
    }
}

fn restore_bodies(
    command: &mut QueueCommand,
    bodies: &mut impl Iterator<Item = Bytes>,
) -> io::Result<()> {
    match command {
        QueueCommand::Batch { commands } => {
            for command in commands {
                restore_bodies(command, bodies)?;
            }
        }
        QueueCommand::Publish {
            bodies: placeholders,
            ..
        } => {
            for body in placeholders {
                *body = bodies.next().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "entry body table is incomplete",
                    )
                })?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::{CommittedLeaderId, EntryPayload, LogId};

    #[test]
    fn entry_envelope_keeps_bodies_out_of_metadata() {
        let entry = Entry {
            log_id: LogId::new(CommittedLeaderId::new(2, 1), 9),
            payload: EntryPayload::Normal(crate::CommandEnvelope::new(QueueCommand::Publish {
                operation_id: 7,
                topic: "events".into(),
                bodies: vec![Bytes::from_static(b"one"), Bytes::from_static(b"two")],
                timestamp_ns: 1,
                available_at_ms: 2,
                partition: Some(0),
                routing_key: None,
            })),
        };
        let encoded = encode(&entry).unwrap();
        let (_, table, body_start) = decode_metadata(&encoded.bytes).unwrap();
        assert_eq!(table.len(), 2);
        assert!(!encoded.bytes[..body_start]
            .windows(3)
            .any(|bytes| bytes == b"one"));
        assert_eq!(&encoded.bytes[body_start..body_start + 3], b"one");
        let decoded = decode(&encoded.bytes).unwrap();
        assert_eq!(
            serde_json::to_vec(&decoded).unwrap(),
            serde_json::to_vec(&entry).unwrap()
        );
    }
}
