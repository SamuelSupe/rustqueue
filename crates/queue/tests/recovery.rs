use rustqueue_queue::{Broker, BrokerConfig};
use std::collections::BTreeSet;
use std::time::Duration;

fn config(path: &std::path::Path) -> BrokerConfig {
    BrokerConfig {
        data_path: path.into(),
        node_id: 37,
        max_segment_bytes: 256,
        max_message_bytes: 1024,
        bootstrap_retention: Duration::from_secs(60),
        ..BrokerConfig::default()
    }
}

#[tokio::test]
async fn durable_fin_req_and_unfinished_delivery_recover_at_least_once() {
    let root = tempfile::tempdir().unwrap();
    let broker = Broker::open(config(root.path())).unwrap();
    broker.create_channel("events", "workers").await.unwrap();
    let ids = broker
        .publish(
            "events",
            vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()],
            Duration::ZERO,
        )
        .await
        .unwrap();
    let delivered = broker
        .fetch_batch("events", "workers", 3, 1024, Duration::ZERO, None)
        .await
        .unwrap();
    assert_eq!(delivered.len(), 3);
    broker.finish("events", "workers", ids[1]).await.unwrap();
    broker
        .requeue("events", "workers", ids[0], Duration::ZERO)
        .await
        .unwrap();
    drop(broker);

    let broker = Broker::open(config(root.path())).unwrap();
    let recovered = broker
        .fetch_batch("events", "workers", 8, 1024, Duration::ZERO, None)
        .await
        .unwrap();
    let actual: BTreeSet<_> = recovered.iter().map(|message| message.id).collect();
    assert_eq!(actual, BTreeSet::from([ids[0], ids[2]]));
    assert_eq!(
        recovered.len(),
        2,
        "a requeued item must not be selected twice in one batch"
    );
    assert_eq!(
        recovered
            .iter()
            .find(|message| message.id == ids[0])
            .unwrap()
            .attempts,
        2,
        "a durable REQ must preserve its attempt count across restart"
    );
    assert_eq!(
        recovered
            .iter()
            .find(|message| message.id == ids[2])
            .unwrap()
            .attempts,
        1,
        "an in-memory lease is intentionally forgotten on restart"
    );
    for message in recovered {
        broker
            .finish("events", "workers", message.id)
            .await
            .unwrap();
    }
    let next = broker
        .publish("events", vec![b"four".to_vec()], Duration::ZERO)
        .await
        .unwrap()[0];
    assert!(
        next > ids[2],
        "a restart must never reuse a persisted broker-scoped ID"
    );
    drop(broker);

    let broker = Broker::open(config(root.path())).unwrap();
    let only_new = broker
        .next_message("events", "workers", None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(only_new.id, next);
}

#[tokio::test]
async fn channels_fan_out_and_ephemeral_channels_do_not_survive_restart() {
    let root = tempfile::tempdir().unwrap();
    let broker = Broker::open(config(root.path())).unwrap();
    let id = broker
        .publish("events", vec![b"bootstrap".to_vec()], Duration::ZERO)
        .await
        .unwrap()[0];
    broker.create_channel("events", "alpha").await.unwrap();
    broker.create_channel("events", "beta").await.unwrap();
    broker
        .create_channel("events", "temporary#ephemeral")
        .await
        .unwrap();
    let alpha = broker
        .next_message("events", "alpha", None)
        .await
        .unwrap()
        .unwrap();
    let beta = broker
        .next_message("events", "beta", None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((alpha.id, beta.id), (id, id));
    broker.finish("events", "alpha", alpha.id).await.unwrap();
    drop(broker);

    let broker = Broker::open(config(root.path())).unwrap();
    assert!(!broker
        .channel_names("events")
        .unwrap()
        .iter()
        .any(|name| name.ends_with("#ephemeral")));
    assert!(broker
        .next_message("events", "alpha", None)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        broker
            .next_message("events", "beta", None)
            .await
            .unwrap()
            .unwrap()
            .id,
        id
    );
}

#[tokio::test]
async fn messages_without_a_durable_channel_survive_gc_and_a_legacy_restart() {
    let root = tempfile::tempdir().unwrap();
    let mut cfg = config(root.path());
    cfg.bootstrap_retention = Duration::ZERO;
    let broker = Broker::open(cfg.clone()).unwrap();
    let ids = broker
        .publish(
            "events",
            vec![vec![1; 80], vec![2; 80], vec![3; 80]],
            Duration::ZERO,
        )
        .await
        .unwrap();
    broker.flush().await.unwrap();

    assert_eq!(broker.compact().await.unwrap(), 0);
    assert_eq!(broker.stats().topics[0].message_count, ids.len() as u64);
    drop(broker);

    let manifest_path = root
        .path()
        .join("topics")
        .join(hex::encode("events"))
        .join("manifest");
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
    assert!(manifest
        .as_object_mut()
        .unwrap()
        .remove("unrouted_from_position")
        .is_some());
    std::fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();

    let broker = Broker::open(cfg).unwrap();
    assert_eq!(broker.compact().await.unwrap(), 0);
    broker.create_channel("events", "workers").await.unwrap();
    let delivered = broker
        .fetch_batch("events", "workers", 8, 1024, Duration::ZERO, None)
        .await
        .unwrap();
    assert_eq!(
        delivered
            .iter()
            .map(|message| message.id)
            .collect::<Vec<_>>(),
        ids
    );
}

#[tokio::test]
async fn messages_after_the_last_durable_channel_is_deleted_survive_gc_and_restart() {
    let root = tempfile::tempdir().unwrap();
    let mut cfg = config(root.path());
    cfg.bootstrap_retention = Duration::ZERO;
    let broker = Broker::open(cfg.clone()).unwrap();
    broker.create_channel("events", "workers").await.unwrap();
    let old = broker
        .publish("events", vec![vec![1; 160]], Duration::ZERO)
        .await
        .unwrap()[0];
    let delivered = broker
        .next_message("events", "workers", None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivered.id, old);
    broker.finish("events", "workers", old).await.unwrap();
    broker.delete_channel("events", "workers").await.unwrap();

    let fresh = broker
        .publish(
            "events",
            vec![vec![2; 80], vec![3; 80], vec![4; 80]],
            Duration::ZERO,
        )
        .await
        .unwrap();
    broker.flush().await.unwrap();
    drop(broker);

    let broker = Broker::open(cfg).unwrap();
    assert!(broker.compact().await.unwrap() > 0);
    assert_eq!(broker.stats().topics[0].message_count, fresh.len() as u64);
    broker
        .create_channel("events", "replacement")
        .await
        .unwrap();
    let delivered = broker
        .fetch_batch("events", "replacement", 8, 1024, Duration::ZERO, None)
        .await
        .unwrap();
    assert_eq!(
        delivered
            .iter()
            .map(|message| message.id)
            .collect::<Vec<_>>(),
        fresh
    );
}

#[tokio::test]
async fn recreated_ephemeral_channel_starts_at_the_current_tail() {
    let root = tempfile::tempdir().unwrap();
    let broker = Broker::open(config(root.path())).unwrap();
    broker
        .create_channel("events", "temporary#ephemeral")
        .await
        .unwrap();
    let first = broker
        .publish("events", vec![b"first".to_vec()], Duration::ZERO)
        .await
        .unwrap()[0];
    assert_eq!(
        broker
            .next_message("events", "temporary#ephemeral", None)
            .await
            .unwrap()
            .unwrap()
            .id,
        first
    );
    broker
        .delete_channel("events", "temporary#ephemeral")
        .await
        .unwrap();
    broker
        .publish("events", vec![b"stale".to_vec()], Duration::ZERO)
        .await
        .unwrap();
    broker
        .create_channel("events", "temporary#ephemeral")
        .await
        .unwrap();
    assert!(broker
        .next_message("events", "temporary#ephemeral", None)
        .await
        .unwrap()
        .is_none());
    let fresh = broker
        .publish("events", vec![b"fresh".to_vec()], Duration::ZERO)
        .await
        .unwrap()[0];
    assert_eq!(
        broker
            .next_message("events", "temporary#ephemeral", None)
            .await
            .unwrap()
            .unwrap()
            .id,
        fresh
    );
}

#[tokio::test]
async fn gc_preserves_positions_and_an_empty_log_reopens_at_the_next_index() {
    let root = tempfile::tempdir().unwrap();
    let mut cfg = config(root.path());
    cfg.bootstrap_retention = Duration::ZERO;
    let broker = Broker::open(cfg.clone()).unwrap();
    broker.create_channel("events", "workers").await.unwrap();
    let ids = broker
        .publish(
            "events",
            vec![vec![1; 80], vec![2; 80], vec![3; 80]],
            Duration::ZERO,
        )
        .await
        .unwrap();
    for message in broker
        .fetch_batch("events", "workers", 3, 1024, Duration::ZERO, None)
        .await
        .unwrap()
    {
        broker
            .finish("events", "workers", message.id)
            .await
            .unwrap();
    }
    assert!(broker.compact().await.unwrap() > 0);
    assert_eq!(broker.stats().topics[0].message_count, 0);
    drop(broker);

    let broker = Broker::open(cfg).unwrap();
    broker.create_channel("events", "late").await.unwrap();
    let fresh = broker
        .publish("events", vec![b"fresh".to_vec()], Duration::ZERO)
        .await
        .unwrap()[0];
    assert!(fresh > ids[2]);
    assert_eq!(
        broker
            .next_message("events", "late", None)
            .await
            .unwrap()
            .unwrap()
            .id,
        fresh
    );
}

#[tokio::test]
async fn protective_eviction_persists_channel_gap_and_audit_before_deleting() {
    let root = tempfile::tempdir().unwrap();
    let broker = Broker::open(config(root.path())).unwrap();
    broker.create_channel("events", "workers").await.unwrap();
    let mut ids = Vec::new();
    for value in 1..=3 {
        ids.push(
            broker
                .publish("events", vec![vec![value; 80]], Duration::ZERO)
                .await
                .unwrap()[0],
        );
    }
    let report = broker.protective_evict_oldest().await.unwrap().unwrap();
    assert_eq!(report.topic, "events");
    assert_eq!(report.messages, 1);
    assert_eq!(report.through_position, 1);
    let channel = &broker.stats().topics[0].channels[0];
    assert_eq!(channel.ack_cursor, 1);
    assert_eq!(channel.message_count, 3);
    assert_eq!(
        std::fs::read_dir(root.path().join("audit"))
            .unwrap()
            .count(),
        1
    );
    drop(broker);

    let broker = Broker::open(config(root.path())).unwrap();
    assert_eq!(broker.stats().topics[0].channels[0].message_count, 3);
    let next = broker
        .next_message("events", "workers", None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.id, ids[1]);
}
