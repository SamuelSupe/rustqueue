pub const FEATURE_LEVEL_BASELINE: u64 = 1;
pub const FEATURE_LEVEL_LARGE_MESSAGES: u64 = 2;
pub const FEATURE_LEVEL_PROTECTIVE_EVICTION: u64 = 3;
pub const FEATURE_LEVEL_FEDERATED_SCHEMA: u64 = 4;
pub const FEATURE_LEVEL_HOME_CELL_ROUTING: u64 = 5;
pub const CURRENT_FEATURE_LEVEL: u64 = FEATURE_LEVEL_HOME_CELL_ROUTING;
pub const BASELINE_MAX_MESSAGE_BYTES: usize = 1024 * 1024;
pub const BASELINE_MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;

pub(crate) fn advertised_feature_level(value: &serde_json::Value) -> u64 {
    value["max_feature_level"]
        .as_u64()
        .unwrap_or(FEATURE_LEVEL_BASELINE)
}

pub(crate) fn required_publish_feature(bodies: &[bytes::Bytes]) -> u64 {
    let total = bodies.iter().map(bytes::Bytes::len).sum::<usize>();
    if bodies
        .iter()
        .any(|body| body.len() > BASELINE_MAX_MESSAGE_BYTES)
        || total > BASELINE_MAX_BATCH_BYTES
    {
        FEATURE_LEVEL_LARGE_MESSAGES
    } else {
        FEATURE_LEVEL_BASELINE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peers_without_capabilities_are_treated_as_baseline() {
        assert_eq!(
            advertised_feature_level(&serde_json::json!({"version": "0.4.0"})),
            FEATURE_LEVEL_BASELINE
        );
    }

    #[test]
    fn large_publish_requires_the_rolling_upgrade_gate() {
        assert_eq!(
            required_publish_feature(&[bytes::Bytes::from_static(b"small")]),
            FEATURE_LEVEL_BASELINE
        );
        assert_eq!(
            required_publish_feature(&[bytes::Bytes::from(vec![
                0;
                BASELINE_MAX_MESSAGE_BYTES + 1
            ])]),
            FEATURE_LEVEL_LARGE_MESSAGES
        );
    }
}
