use bytes::Bytes;
use thiserror::Error;

pub const MAX_MPUB_MESSAGES: usize = 65_536;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MpubError {
    #[error("{0}")]
    BadBody(&'static str),
    #[error("{0}")]
    BadMessage(&'static str),
}

impl MpubError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::BadBody(_) => "E_BAD_BODY",
            Self::BadMessage(_) => "E_BAD_MESSAGE",
        }
    }
}

pub fn parse_mpub_body(body: &[u8], max_message_bytes: usize) -> Result<Vec<Vec<u8>>, MpubError> {
    parse_mpub_bytes(Bytes::copy_from_slice(body), max_message_bytes).map(|messages| {
        messages
            .into_iter()
            .map(|message| message.to_vec())
            .collect()
    })
}

pub fn parse_mpub_bytes(body: Bytes, max_message_bytes: usize) -> Result<Vec<Bytes>, MpubError> {
    if body.len() < 4 {
        return Err(MpubError::BadBody("missing batch count"));
    }
    let count = u32::from_be_bytes(body[..4].try_into().unwrap()) as usize;
    if count == 0 || count > MAX_MPUB_MESSAGES {
        return Err(MpubError::BadBody("invalid batch count"));
    }
    let mut offset = 4usize;
    let mut messages = Vec::with_capacity(count);
    for _ in 0..count {
        if body.len().saturating_sub(offset) < 4 {
            return Err(MpubError::BadMessage("truncated message length"));
        }
        let length = u32::from_be_bytes(body[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let end = offset.saturating_add(length);
        if length == 0 || length > max_message_bytes || end > body.len() {
            return Err(MpubError::BadMessage("invalid message length"));
        }
        messages.push(body.slice(offset..end));
        offset = end;
    }
    if offset != body.len() {
        return Err(MpubError::BadBody("batch contains trailing bytes"));
    }
    Ok(messages)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_batch_atomically() {
        let mut body = 2u32.to_be_bytes().to_vec();
        body.extend_from_slice(&3u32.to_be_bytes());
        body.extend_from_slice(b"one");
        body.extend_from_slice(&3u32.to_be_bytes());
        body.extend_from_slice(b"two");
        assert_eq!(
            parse_mpub_body(&body, 10).unwrap(),
            vec![b"one".to_vec(), b"two".to_vec()]
        );
        body.push(0);
        assert!(parse_mpub_body(&body, 10).is_err());
    }

    #[test]
    fn byte_parser_slices_the_original_allocation() {
        let mut body = 1u32.to_be_bytes().to_vec();
        body.extend_from_slice(&4u32.to_be_bytes());
        body.extend_from_slice(b"body");
        let body = Bytes::from(body);
        let message = parse_mpub_bytes(body.clone(), 10).unwrap().pop().unwrap();
        assert_eq!(message, Bytes::from_static(b"body"));
        assert_eq!(message.as_ptr(), body[8..].as_ptr());
    }

    #[test]
    fn rejects_batches_that_would_overflow_durable_entry_metadata() {
        let body = Bytes::copy_from_slice(&((MAX_MPUB_MESSAGES + 1) as u32).to_be_bytes());
        assert_eq!(
            parse_mpub_bytes(body, 1),
            Err(MpubError::BadBody("invalid batch count"))
        );
    }
}
