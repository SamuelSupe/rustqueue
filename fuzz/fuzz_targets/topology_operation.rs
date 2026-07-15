#![no_main]

use libfuzzer_sys::fuzz_target;
use rustqueue_consensus::{ClusterMetadata, MaintenanceOperation};

fuzz_target!(|data: &[u8]| {
    let _ = serde_json::from_slice::<MaintenanceOperation>(data);
    let _ = serde_json::from_slice::<ClusterMetadata>(data);
});
