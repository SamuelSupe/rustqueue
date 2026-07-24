use super::*;

#[test]
fn example_configuration_is_valid() {
    let config: Config =
        toml::from_str(include_str!("../../../../rustqueue.example.toml")).unwrap();
    config.validate().unwrap();
}

#[test]
fn default_bootstrap_window_covers_official_lookup_poll_jitter() {
    assert_eq!(Config::default().queue.bootstrap_retention_seconds, 90);
    assert!(Config::default().queue.bootstrap_retention_seconds >= 78);
}

#[test]
fn maintenance_has_a_startup_quiet_period() {
    assert_eq!(
        Config::default().storage.maintenance_startup_delay_seconds,
        30
    );
}

#[test]
fn accepts_the_documented_large_message_limits() {
    let mut config = Config::default();
    config.storage.feature_level = 2;
    config.storage.max_segment_bytes = 256 * 1024 * 1024;
    config.queue.max_message_bytes = MAX_SUPPORTED_MESSAGE_BYTES;
    config.limits.max_body_bytes = MAX_SUPPORTED_BATCH_BYTES;
    config.limits.connection_publish_inflight_bytes = 160 * 1024 * 1024;
    config.limits.node_publish_inflight_bytes = 1024 * 1024 * 1024;
    config.limits.connection_delivery_inflight_bytes = 128 * 1024 * 1024;
    config.limits.node_delivery_inflight_bytes = 1024 * 1024 * 1024;
    config.validate().unwrap();
}

#[test]
fn binary_capabilities_advertise_protocol_limits() {
    let capabilities = serde_json::to_value(runtime_capabilities()).unwrap();
    assert_eq!(
        capabilities["maximum_message_bytes"],
        MAX_SUPPORTED_MESSAGE_BYTES
    );
    assert_eq!(
        capabilities["maximum_batch_bytes"],
        MAX_SUPPORTED_BATCH_BYTES
    );
}

#[test]
fn kodo_cleanup_is_disabled_until_cluster_wide_deletion_is_atomic() {
    let root = tempfile::tempdir().unwrap();
    let cleanup = root.path().join("cleanup-token");
    let admin = root.path().join("admin-token");
    std::fs::write(&cleanup, "cleanup-secret").unwrap();
    std::fs::write(&admin, "cleanup-secret").unwrap();
    let mut config = Config::default();
    config.security.kodo_cleanup_enabled = true;
    assert!(config.validate().is_err());
    config.network.kodo_http_address = Some("0.0.0.0:4152".parse().unwrap());
    config.security.kodo_cleanup_token_file = Some(cleanup);
    config.security.admin_token_file = Some(admin.clone());
    assert!(config.validate().is_err());
    std::fs::write(admin, "admin-secret").unwrap();
    let error = config.validate().unwrap_err();
    assert!(error.to_string().contains("cluster-wide atomic deletion"));
}

#[test]
fn large_record_contract_requires_storage_feature_level_two() {
    let mut config = Config::default();
    config.storage.max_segment_bytes = 256 * 1024 * 1024;
    config.queue.max_message_bytes = MAX_SUPPORTED_MESSAGE_BYTES;
    config.limits.max_body_bytes = MAX_SUPPORTED_BATCH_BYTES;
    config.limits.connection_publish_inflight_bytes = 160 * 1024 * 1024;
    config.limits.node_publish_inflight_bytes = 1024 * 1024 * 1024;
    config.limits.connection_delivery_inflight_bytes = 128 * 1024 * 1024;
    config.limits.node_delivery_inflight_bytes = 1024 * 1024 * 1024;
    assert!(config.validate().is_err());

    config.storage.feature_level = 2;
    config.validate().unwrap();
}

#[test]
fn feature_level_two_keeps_enough_budget_to_read_retained_large_messages() {
    let mut config = Config::default();
    config.storage.feature_level = 2;
    assert!(config.validate().is_err());
    config.limits.connection_delivery_inflight_bytes = MAX_SUPPORTED_MESSAGE_BYTES;
    config.limits.node_delivery_inflight_bytes = 2 * MAX_SUPPORTED_MESSAGE_BYTES;
    config.validate().unwrap();
}

#[test]
fn large_publish_body_requires_matching_inflight_budget() {
    let mut config = Config::default();
    config.storage.feature_level = 2;
    config.storage.max_segment_bytes = 256 * 1024 * 1024;
    config.queue.max_message_bytes = MAX_SUPPORTED_MESSAGE_BYTES;
    config.limits.max_body_bytes = MAX_SUPPORTED_MESSAGE_BYTES;
    config.limits.connection_delivery_inflight_bytes = MAX_SUPPORTED_MESSAGE_BYTES;
    config.limits.node_delivery_inflight_bytes = 2 * MAX_SUPPORTED_MESSAGE_BYTES;
    assert!(config.validate().is_err());

    config.limits.connection_publish_inflight_bytes = 160 * 1024 * 1024;
    config.limits.node_publish_inflight_bytes = 512 * 1024 * 1024;
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
fn rejects_zero_protocol_capacity_and_timeouts() {
    let mut config = Config::default();
    config.limits.max_connections = 0;
    assert!(config.validate().is_err());

    let mut config = Config::default();
    config.limits.max_connections = 1;
    config.limits.max_rdy_count = 0;
    assert!(config.validate().is_err());

    let mut config = Config::default();
    config.limits.max_rdy_count = 1;
    config.limits.tcp_command_timeout_ms = 0;
    assert!(config.validate().is_err());

    let mut config = Config::default();
    config.limits.http_body_timeout_ms = 0;
    assert!(config.validate().is_err());

    let mut config = Config::default();
    config.limits.http_body_timeout_ms = 1;
    config.queue.message_timeout_ms = config.queue.max_message_timeout_ms + 1;
    assert!(config.validate().is_err());

    let mut config = Config::default();
    config.limits.max_heartbeat_interval_ms = i64::MAX as u64 + 1;
    assert!(config.validate().is_err());

    let mut config = Config::default();
    config.limits.max_output_buffer_timeout_ms = i64::MAX as u64 + 1;
    assert!(config.validate().is_err());
}

#[test]
fn rejects_an_unbounded_detailed_metric_configuration() {
    let mut config = Config::default();
    config.metrics.max_detailed_series = 0;
    assert!(config.validate().is_err());
}

#[test]
fn delivery_budget_must_fit_a_message_and_the_node_budget() {
    let mut config = Config::default();
    config.limits.connection_delivery_inflight_bytes = config.queue.max_message_bytes - 1;
    assert!(config.validate().is_err());

    config.limits.connection_delivery_inflight_bytes = config.queue.max_message_bytes;
    config.limits.node_delivery_inflight_bytes = config.queue.max_message_bytes - 1;
    assert!(config.validate().is_err());
}
