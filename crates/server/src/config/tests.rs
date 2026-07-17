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

#[test]
fn rejects_unbounded_topic_worker_configuration() {
    let mut config = Config::default();
    config.queue.max_publish_workers = 0;
    assert!(config.validate().is_err());
    config.queue.max_publish_workers = 1;
    config.queue.max_topics = 0;
    assert!(config.validate().is_err());
}

#[test]
fn rejects_an_unbounded_detailed_metric_configuration() {
    let mut config = Config::default();
    config.metrics.max_detailed_series = 0;
    assert!(config.validate().is_err());
}
