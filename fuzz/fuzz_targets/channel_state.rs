#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let split = data
        .get(..4)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes)
        .map_or(0, |value| value as usize)
        .min(data.len());
    rustqueue_queue::fuzz_channel_state(&data[..split], &data[split..]);

    let checkpoint_body = br#"{"format":7,"name":"workers","barrier_position":0,"ack_floor_position":0,"acknowledged":[],"requeued_until":{},"attempts":{},"paused":false,"ephemeral":false}"#;
    let checkpoint = envelope(b"RCC7", checkpoint_body);
    let mut wal = envelope(b"RCW7", &[3, data.first().copied().unwrap_or_default() & 1]);
    if !data.is_empty() {
        wal.extend_from_slice(&envelope(b"RCW7", &data[..data.len().min(1024 * 1024)]));
    }
    rustqueue_queue::fuzz_channel_state(&checkpoint, &wal);
});

fn envelope(magic: &[u8; 4], body: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(12 + body.len());
    bytes.extend_from_slice(magic);
    bytes.extend_from_slice(&(body.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&crc32c::crc32c(body).to_be_bytes());
    bytes.extend_from_slice(body);
    bytes
}
