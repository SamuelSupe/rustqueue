use super::io::SEQUENCE_RESERVATION;
use super::*;
use crate::outbox::OutboxEntry;
use futures::future::join_all;
use std::collections::HashSet;
use tempfile::tempdir;

#[tokio::test]
async fn startup_replays_dlq_outbox_before_finishing_the_source() {
    let root = tempdir().unwrap();
    let config = BrokerConfig {
        data_path: root.path().into(),
        bootstrap_retention: Duration::from_secs(60),
        ..BrokerConfig::default()
    };
    let broker = Broker::open(config.clone()).unwrap();
    broker.create_channel("events", "workers").await.unwrap();
    let id = broker
        .publish("events", vec![b"poison".to_vec()], Duration::ZERO)
        .await
        .unwrap()[0];
    broker
        .next_message("events", "workers", None)
        .await
        .unwrap()
        .unwrap();
    let entry = OutboxEntry {
        source_topic: "events".into(),
        source_channel: "workers".into(),
        message_id: id,
        target_topic: "events.workers.DLQ".into(),
        body: bytes::Bytes::from_static(b"poison"),
    };
    crate::outbox::store(&root.path().join("dlq-outbox"), &entry).unwrap();
    drop(broker);

    let broker = Broker::open(config).unwrap();
    assert_eq!(
        std::fs::read_dir(root.path().join("dlq-outbox"))
            .unwrap()
            .count(),
        0
    );
    assert!(broker
        .next_message("events", "workers", None)
        .await
        .unwrap()
        .is_none());
    broker
        .create_channel("events.workers.DLQ", "inspect")
        .await
        .unwrap();
    let dlq = broker
        .next_message("events.workers.DLQ", "inspect", None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&*dlq.body, b"poison");
}

#[tokio::test]
async fn restart_never_reuses_ids_from_a_reserved_sequence_block() {
    let root = tempdir().unwrap();
    let config = BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    };
    let broker = Broker::open(config.clone()).unwrap();
    let first = broker
        .publish("events", vec![b"one".to_vec()], Duration::ZERO)
        .await
        .unwrap()[0];
    drop(broker);

    let broker = Broker::open(config).unwrap();
    let second = broker
        .publish("events", vec![b"two".to_vec()], Duration::ZERO)
        .await
        .unwrap()[0];
    assert!(second > first);
    assert_eq!(second & ((1u64 << 48) - 1), SEQUENCE_RESERVATION + 1);
}

#[tokio::test]
async fn concurrent_publishes_share_a_durable_group_commit() {
    let root = tempdir().unwrap();
    let config = BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    };
    let broker = Broker::open(config.clone()).unwrap();
    let publishes = (0..32).map(|ordinal| {
        let broker = broker.clone();
        async move {
            broker
                .publish(
                    "events",
                    vec![format!("message-{ordinal}").into_bytes()],
                    Duration::ZERO,
                )
                .await
        }
    });
    let results = join_all(publishes).await;
    let ids: HashSet<_> = results
        .into_iter()
        .map(|result| result.unwrap()[0])
        .collect();
    assert_eq!(ids.len(), 32);

    let commit = broker.stats().publish_group_commit;
    assert_eq!(commit.requests, 32);
    assert!(commit.commits < commit.requests);
    assert!(commit.max_batch_requests > 1);
    drop(broker);

    let broker = Broker::open(config).unwrap();
    assert_eq!(broker.stats().topics[0].message_count, 32);
}

#[tokio::test]
async fn a_rejected_request_does_not_fail_other_requests_in_the_group() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        max_backlog_messages: 1,
        ..BrokerConfig::default()
    })
    .unwrap();
    let (first, second) = tokio::join!(
        broker.publish("events", vec![b"first".to_vec()], Duration::ZERO),
        broker.publish("events", vec![b"second".to_vec()], Duration::ZERO),
    );
    let results = [first, second];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(BrokerError::BacklogLimit)))
            .count(),
        1
    );
    assert!(broker.storage_healthy());
    let stats = broker.stats();
    assert_eq!(stats.publish_group_commit.commits, 1);
    assert_eq!(stats.publish_group_commit.requests, 1);
    assert_eq!(stats.topics[0].message_count, 1);
}
