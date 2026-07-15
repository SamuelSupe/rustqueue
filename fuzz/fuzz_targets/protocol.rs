#![no_main]

use libfuzzer_sys::fuzz_target;
use rustqueue_protocol::{parse_mpub_body, Command, IdentifyRequest};

fuzz_target!(|data: &[u8]| {
    let _ = Command::parse(data);
    let _ = serde_json::from_slice::<IdentifyRequest>(data);
    let _ = parse_mpub_body(data, 1024 * 1024);
});
