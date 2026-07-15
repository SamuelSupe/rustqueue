use super::*;

#[test]
fn example_configuration_is_valid() {
    let config: Config =
        toml::from_str(include_str!("../../../../rustqueue.example.toml")).unwrap();
    config.validate().unwrap();
}

#[test]
fn cluster_examples_have_three_https_nodes() {
    for source in [
        include_str!("../../../../deploy/node-1.toml"),
        include_str!("../../../../deploy/node-2.toml"),
        include_str!("../../../../deploy/node-3.toml"),
    ] {
        let config: Config = toml::from_str(source).unwrap();
        assert_eq!(config.cluster.nodes.len(), 3);
        assert!(config
            .cluster
            .nodes
            .values()
            .all(|node| node.raft_address.starts_with("https://")));
    }
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
