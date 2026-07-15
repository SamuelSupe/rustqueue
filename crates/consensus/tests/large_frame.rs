use bytes::Bytes;
use rustqueue_consensus::{
    decode_frame_with_limit, encode_frame_with_limit, QueueCommand, INTERNAL_WRITE_FRAME_BYTES,
};

#[test]
fn twenty_mebibyte_publish_fits_the_internal_write_frame() {
    let command = QueueCommand::Publish {
        operation_id: 1,
        topic: "large".into(),
        bodies: vec![Bytes::from(vec![0x5a; 20 * 1024 * 1024])],
        timestamp_ns: 0,
        available_at_ms: 0,
        partition: Some(0),
        routing_key: None,
    };
    let encoded = encode_frame_with_limit(&command, INTERNAL_WRITE_FRAME_BYTES).unwrap();
    let decoded: QueueCommand =
        decode_frame_with_limit(&encoded, INTERNAL_WRITE_FRAME_BYTES).unwrap();
    assert!(
        matches!(decoded, QueueCommand::Publish { bodies, .. } if bodies[0].len() == 20 * 1024 * 1024)
    );
}
