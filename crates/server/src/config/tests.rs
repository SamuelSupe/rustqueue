use super::*;

#[test]
fn example_configuration_is_valid() {
    let config: Config =
        toml::from_str(include_str!("../../../../rustqueue.example.toml")).unwrap();
    config.validate().unwrap();
}

#[test]
fn accepts_the_documented_large_message_limits() {
    let mut config = Config::default();
    config.queue.max_message_bytes = MAX_SUPPORTED_MESSAGE_BYTES;
    config.limits.max_body_bytes = MAX_SUPPORTED_BATCH_BYTES;
    config.validate().unwrap();
}

#[test]
fn rejects_limits_above_the_stable_wire_contract() {
    let mut config = Config::default();
    config.queue.max_message_bytes = MAX_SUPPORTED_MESSAGE_BYTES + 1;
    config.limits.max_body_bytes = MAX_SUPPORTED_BATCH_BYTES;
    assert!(config.validate().is_err());

    config.queue.max_message_bytes = MAX_SUPPORTED_MESSAGE_BYTES;
    config.limits.max_body_bytes = MAX_SUPPORTED_BATCH_BYTES + 1;
    config.limits.connection_publish_inflight_bytes = config.limits.max_body_bytes;
    assert!(config.validate().is_err());
}
