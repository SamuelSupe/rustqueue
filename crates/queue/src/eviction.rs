use crate::metadata::store_atomic;
use crate::BrokerError;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProtectiveEviction {
    pub topic: String,
    pub first_position: u64,
    pub through_position: u64,
    pub messages: u64,
    pub segment: PathBuf,
    pub created_at_ms: u64,
}

pub(crate) fn write_intent(
    directory: &Path,
    report: &ProtectiveEviction,
) -> Result<(), BrokerError> {
    let name = format!(
        "eviction-{:020}-{}-{:020}.json",
        report.created_at_ms,
        hex::encode(report.topic.as_bytes()),
        report.through_position,
    );
    store_atomic(&directory.join(name), report)?;
    Ok(())
}
