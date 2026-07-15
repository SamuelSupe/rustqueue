use crate::network_metrics::{network_metrics, RpcKind};
use bincode::Options;
use reqwest::header::{ACCEPT, CONTENT_TYPE};
use serde::de::DeserializeOwned;
use serde::Serialize;
use thiserror::Error;

const MAGIC: &[u8; 4] = b"RQR6";
pub const INTERNAL_RPC_FORMAT: u16 = 6;
pub const INTERNAL_RPC_VERSION: u8 = 1;
const HEADER_BYTES: usize = 16;
const MAX_FRAME_BYTES: usize = 1024 * 1024 * 1024;

pub const INTERNAL_BINARY_CONTENT_TYPE: &str = "application/x-rustqueue-rpc";

#[derive(Debug, Error)]
pub enum WireError {
    #[error("binary RPC frame is truncated")]
    Truncated,
    #[error("binary RPC frame has invalid magic")]
    Magic,
    #[error("unsupported binary RPC version {0}")]
    Version(u8),
    #[error("binary RPC frame flags are invalid")]
    Flags,
    #[error("binary RPC frame exceeds the configured limit")]
    TooLarge,
    #[error("binary RPC frame length does not match its header")]
    Length,
    #[error("binary RPC frame checksum mismatch")]
    Checksum,
    #[error("binary RPC codec error: {0}")]
    Codec(#[from] Box<bincode::ErrorKind>),
}

pub fn encode_frame<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, WireError> {
    encode_frame_with_limit(value, MAX_FRAME_BYTES)
}

pub fn encoded_frame_len<T: Serialize + ?Sized>(value: &T) -> Result<usize, WireError> {
    let payload = codec().serialized_size(value)?;
    let payload: usize = payload.try_into().map_err(|_| WireError::TooLarge)?;
    HEADER_BYTES
        .checked_add(payload)
        .filter(|bytes| *bytes <= MAX_FRAME_BYTES)
        .ok_or(WireError::TooLarge)
}

pub fn encode_frame_with_limit<T: Serialize + ?Sized>(
    value: &T,
    max_frame_bytes: usize,
) -> Result<Vec<u8>, WireError> {
    let payload_limit = max_frame_bytes
        .checked_sub(HEADER_BYTES)
        .ok_or(WireError::TooLarge)?;
    let mut frame = Vec::with_capacity(HEADER_BYTES + 1024);
    frame.resize(HEADER_BYTES, 0);
    codec()
        .with_limit(payload_limit as u64)
        .serialize_into(&mut frame, value)?;
    let payload_len = frame.len().saturating_sub(HEADER_BYTES);
    if frame.len() > max_frame_bytes || payload_len > u32::MAX as usize {
        return Err(WireError::TooLarge);
    }
    frame[..4].copy_from_slice(MAGIC);
    frame[4] = INTERNAL_RPC_VERSION;
    frame[5] = 0;
    frame[6..8].copy_from_slice(&0u16.to_be_bytes());
    frame[8..12].copy_from_slice(&(payload_len as u32).to_be_bytes());
    let checksum = crc32c::crc32c(&frame[HEADER_BYTES..]);
    frame[12..16].copy_from_slice(&checksum.to_be_bytes());
    Ok(frame)
}

pub fn decode_frame<T: DeserializeOwned>(frame: &[u8]) -> Result<T, WireError> {
    decode_frame_with_limit(frame, MAX_FRAME_BYTES)
}

pub fn decode_frame_with_limit<T: DeserializeOwned>(
    frame: &[u8],
    max_frame_bytes: usize,
) -> Result<T, WireError> {
    if frame.len() > max_frame_bytes {
        return Err(WireError::TooLarge);
    }
    if frame.len() < HEADER_BYTES {
        return Err(WireError::Truncated);
    }
    if &frame[..4] != MAGIC {
        return Err(WireError::Magic);
    }
    if frame[4] != INTERNAL_RPC_VERSION {
        return Err(WireError::Version(frame[4]));
    }
    if frame[5] != 0 || frame[6..8] != [0, 0] {
        return Err(WireError::Flags);
    }
    let payload_len = u32::from_be_bytes(frame[8..12].try_into().unwrap()) as usize;
    if HEADER_BYTES.saturating_add(payload_len) > max_frame_bytes {
        return Err(WireError::TooLarge);
    }
    if frame.len() != HEADER_BYTES + payload_len {
        return Err(WireError::Length);
    }
    let payload = &frame[HEADER_BYTES..];
    let expected_crc = u32::from_be_bytes(frame[12..16].try_into().unwrap());
    if crc32c::crc32c(payload) != expected_crc {
        return Err(WireError::Checksum);
    }
    Ok(codec()
        .with_limit(payload_len as u64)
        .reject_trailing_bytes()
        .deserialize(payload)?)
}

pub async fn post_binary<Req, Resp>(
    client: &reqwest::Client,
    url: impl reqwest::IntoUrl,
    request: &Req,
) -> anyhow::Result<Resp>
where
    Req: Serialize + ?Sized,
    Resp: DeserializeOwned,
{
    post_binary_limited(client, url, request, MAX_FRAME_BYTES, MAX_FRAME_BYTES).await
}

pub async fn post_binary_limited<Req, Resp>(
    client: &reqwest::Client,
    url: impl reqwest::IntoUrl,
    request: &Req,
    request_limit: usize,
    response_limit: usize,
) -> anyhow::Result<Resp>
where
    Req: Serialize + ?Sized,
    Resp: DeserializeOwned,
{
    let body = encode_frame_with_limit(request, request_limit)?;
    network_metrics().record_request(RpcKind::Control, body.len());
    let response = client
        .post(url)
        .header(CONTENT_TYPE, INTERNAL_BINARY_CONTENT_TYPE)
        .header(ACCEPT, INTERNAL_BINARY_CONTENT_TYPE)
        .body(body)
        .send()
        .await?
        .error_for_status()?;
    if response
        .content_length()
        .is_some_and(|length| length > response_limit as u64)
    {
        anyhow::bail!("internal RPC response exceeds endpoint limit");
    }
    let bytes = response.bytes().await?;
    network_metrics().record_response(RpcKind::Control, bytes.len());
    Ok(decode_frame_with_limit(&bytes, response_limit)?)
}

fn codec() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_little_endian()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CommandEnvelope, QueueCommand};
    use serde::Deserialize;

    #[derive(Deserialize, Serialize)]
    enum LegacyQueueCommand {
        Batch {
            commands: Vec<LegacyQueueCommand>,
        },
        Publish {
            operation_id: u64,
            topic: String,
            bodies: Vec<Vec<u8>>,
            timestamp_ns: i64,
            available_at_ms: i64,
            partition: Option<u16>,
            routing_key: Option<Vec<u8>>,
        },
    }

    #[test]
    fn rejects_corruption_version_and_endpoint_limit() {
        let mut frame = encode_frame(&vec![1u64, 2, 3]).unwrap();
        frame[4] = 2;
        assert!(matches!(
            decode_frame::<Vec<u64>>(&frame),
            Err(WireError::Version(2))
        ));
        frame[4] = INTERNAL_RPC_VERSION;
        *frame.last_mut().unwrap() ^= 1;
        assert!(matches!(
            decode_frame::<Vec<u64>>(&frame),
            Err(WireError::Checksum)
        ));
        assert!(matches!(
            encode_frame_with_limit(&vec![0u8; 128], 64),
            Err(WireError::Codec(_)) | Err(WireError::TooLarge)
        ));
    }

    #[test]
    fn round_trips_empty_control_request() {
        let frame = encode_frame(&()).unwrap();
        let _: () = decode_frame(&frame).unwrap();
    }

    #[test]
    fn round_trips_default_queue_response() {
        let frame = encode_frame(&crate::QueueResponse::default()).unwrap();
        let response: crate::QueueResponse = decode_frame(&frame).unwrap();
        assert!(response.results.is_empty());
    }

    #[test]
    fn publish_payload_is_not_expanded_like_json() {
        let body: Vec<u8> = (0..1024).map(|value| value as u8).collect();
        let command = QueueCommand::Publish {
            operation_id: 1,
            topic: "events".into(),
            bodies: vec![bytes::Bytes::from(body)],
            timestamp_ns: 0,
            available_at_ms: 0,
            partition: Some(0),
            routing_key: None,
        };
        let envelope = CommandEnvelope::new(command);
        let binary = encode_frame(&envelope).unwrap();
        let json = serde_json::to_vec(&envelope).unwrap();
        assert!(
            binary.len() < 1_200,
            "binary frame was {} bytes",
            binary.len()
        );
        assert!(binary.len() * 2 < json.len());
        let decoded: CommandEnvelope = decode_frame(&binary).unwrap();
        decoded.validate().unwrap();
        assert!(
            matches!(decoded.command, QueueCommand::Publish { bodies, .. } if bodies[0].len() == 1024)
        );
    }

    #[test]
    fn bytes_publish_remains_wire_compatible_with_the_v3_vec_body() {
        let legacy = LegacyQueueCommand::Publish {
            operation_id: 1,
            topic: "events".into(),
            bodies: vec![b"body".to_vec()],
            timestamp_ns: 2,
            available_at_ms: 3,
            partition: Some(0),
            routing_key: None,
        };
        let current = QueueCommand::Publish {
            operation_id: 1,
            topic: "events".into(),
            bodies: vec![bytes::Bytes::from_static(b"body")],
            timestamp_ns: 2,
            available_at_ms: 3,
            partition: Some(0),
            routing_key: None,
        };
        assert_eq!(
            encode_frame(&legacy).unwrap(),
            encode_frame(&current).unwrap()
        );
        let decoded: LegacyQueueCommand = decode_frame(&encode_frame(&current).unwrap()).unwrap();
        assert!(
            matches!(decoded, LegacyQueueCommand::Publish { bodies, .. } if bodies == [b"body"])
        );
    }
}
