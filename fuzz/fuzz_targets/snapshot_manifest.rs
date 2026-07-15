#![no_main]

use libfuzzer_sys::fuzz_target;
use rustqueue_storage::{GenerationManifest, SnapshotManifest};

fuzz_target!(|data: &[u8]| {
    let _ = serde_json::from_slice::<SnapshotManifest>(data);
    let _ = serde_json::from_slice::<GenerationManifest>(data);
});
