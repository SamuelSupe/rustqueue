#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    rustqueue_queue::fuzz_topic_manifest(data);
    let name: String = data
        .iter()
        .take(64)
        .map(|byte| char::from(b'a' + byte % 26))
        .collect();
    let structured = serde_json::json!({
        "format": 7,
        "name": name,
        "paused": data.first().is_some_and(|byte| byte & 1 == 1),
        "deleted": false,
        "next_position": data.len(),
    });
    rustqueue_queue::fuzz_topic_manifest(&serde_json::to_vec(&structured).unwrap());
});
