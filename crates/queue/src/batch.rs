use crate::model::StoredMessage;
use rustqueue_protocol::MAX_MPUB_MESSAGES;
use rustqueue_storage::PayloadRef;
use std::io;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

const ITEM_HEADER: usize = 8 + 8 + 8 + 4;

#[derive(Clone, Copy)]
pub struct MessageHeader {
    pub id: u64,
    pub timestamp_ns: i64,
    pub available_at_ms: i64,
}

pub fn encode<B>(
    headers: &[MessageHeader],
    bodies: &[B],
) -> io::Result<(Vec<u8>, Vec<Range<usize>>)>
where
    B: AsRef<[u8]>,
{
    if headers.len() != bodies.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "message header and body counts differ",
        ));
    }
    if bodies.len() > MAX_MPUB_MESSAGES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "batch message count exceeds limit",
        ));
    }
    let capacity = 4usize.saturating_add(
        bodies
            .iter()
            .map(|body| ITEM_HEADER.saturating_add(body.as_ref().len()))
            .sum(),
    );
    let mut output = Vec::with_capacity(capacity);
    let mut ranges = Vec::with_capacity(bodies.len());
    output.extend_from_slice(&(bodies.len() as u32).to_be_bytes());
    for (header, body) in headers.iter().zip(bodies) {
        let body = body.as_ref();
        let length: u32 = body
            .len()
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "message too large"))?;
        output.extend_from_slice(&header.id.to_be_bytes());
        output.extend_from_slice(&header.timestamp_ns.to_be_bytes());
        output.extend_from_slice(&header.available_at_ms.to_be_bytes());
        output.extend_from_slice(&length.to_be_bytes());
        let start = output.len();
        output.extend_from_slice(body);
        ranges.push(start..output.len());
    }
    Ok((output, ranges))
}

pub fn decode_refs(
    mut input: &[u8],
    log_index: u64,
    path: &Arc<PathBuf>,
    payload_offset: u64,
) -> io::Result<Vec<StoredMessage>> {
    let original_len = input.len();
    let count = take_u32(&mut input)? as usize;
    if count > MAX_MPUB_MESSAGES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "batch message count exceeds limit",
        ));
    }
    let mut messages = Vec::with_capacity(count);
    let path = Arc::clone(path);
    for batch_ordinal in 0..count {
        if input.len() < ITEM_HEADER {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated batch item",
            ));
        }
        let id = take_u64(&mut input)?;
        let timestamp_ns = take_i64(&mut input)?;
        let available_at_ms = take_i64(&mut input)?;
        let length = take_u32(&mut input)? as usize;
        if input.len() < length {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated batch body",
            ));
        }
        let consumed = original_len - input.len();
        let body = &input[..length];
        messages.push(StoredMessage {
            id,
            timestamp_ns,
            available_at_ms,
            log_index,
            batch_ordinal: batch_ordinal as u32,
            payload: PayloadRef {
                path: Arc::clone(&path),
                offset: payload_offset.saturating_add(consumed as u64),
                len: length as u32,
                crc32c: crc32c::crc32c(body),
            },
        });
        input = &input[length..];
    }
    if !input.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "batch contains trailing bytes",
        ));
    }
    Ok(messages)
}

fn take_u32(input: &mut &[u8]) -> io::Result<u32> {
    let bytes = take::<4>(input)?;
    Ok(u32::from_be_bytes(bytes))
}

fn take_u64(input: &mut &[u8]) -> io::Result<u64> {
    let bytes = take::<8>(input)?;
    Ok(u64::from_be_bytes(bytes))
}

fn take_i64(input: &mut &[u8]) -> io::Result<i64> {
    let bytes = take::<8>(input)?;
    Ok(i64::from_be_bytes(bytes))
}

fn take<const N: usize>(input: &mut &[u8]) -> io::Result<[u8; N]> {
    if input.len() < N {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated batch header",
        ));
    }
    let output = input[..N].try_into().unwrap();
    *input = &input[N..];
    Ok(output)
}
