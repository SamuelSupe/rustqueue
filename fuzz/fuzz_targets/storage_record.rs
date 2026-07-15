#![no_main]

use libfuzzer_sys::fuzz_target;
use rustqueue_storage::{Record, HEADER_LEN, MAX_RECORD_BYTES};

fuzz_target!(|data: &[u8]| {
    if data.len() < HEADER_LEN {
        return;
    }
    let mut header = [0; HEADER_LEN];
    header.copy_from_slice(&data[..HEADER_LEN]);
    let payload = data[HEADER_LEN..]
        .iter()
        .copied()
        .take(MAX_RECORD_BYTES)
        .collect();
    let _ = Record::payload_len(&header);
    let _ = Record::decode(&header, payload);
});
