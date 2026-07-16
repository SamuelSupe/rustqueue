#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = serde_json::from_slice::<rustqueue_discovery::BrokerRegistry>(data);
    let host: String = data
        .iter()
        .take(64)
        .map(|byte| char::from(b'a' + byte % 26))
        .collect();
    let structured = serde_json::json!({
        "format": 7,
        "revision": data.len(),
        "node_id": 1,
        "ready": true,
        "publish_ready": true,
        "consume_ready": true,
        "broadcast_address": host,
        "tcp_port": 4150,
        "http_port": 4151,
        "topics": [],
    });
    let _ = serde_json::from_value::<rustqueue_discovery::BrokerRegistry>(structured);
});
