use super::io::SEQUENCE_RESERVATION;
use super::*;
use crate::outbox::OutboxEntry;
use crate::{ManagementFenceSnapshot, TopicManagementAction};
use futures::future::join_all;
use std::collections::{BTreeMap, HashSet};
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
async fn topic_reopens_from_sealed_segment_recovery_metadata() {
    let root = tempdir().unwrap();
    let config = BrokerConfig {
        data_path: root.path().into(),
        max_segment_bytes: 100,
        ..BrokerConfig::default()
    };
    let broker = Broker::open(config.clone()).unwrap();
    broker.create_channel("events", "workers").await.unwrap();
    broker
        .publish("events", vec![vec![1; 20]], Duration::ZERO)
        .await
        .unwrap();
    broker
        .publish("events", vec![vec![2; 20]], Duration::ZERO)
        .await
        .unwrap();
    drop(broker);
    let segment_directory = root
        .path()
        .join("topics")
        .join(hex::encode("events"))
        .join("segments");
    assert_eq!(
        std::fs::read_dir(&segment_directory)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "rqidx"))
            .count(),
        1
    );

    let broker = Broker::open(config).unwrap();
    let deliveries = broker
        .fetch_batch("events", "workers", 2, usize::MAX, Duration::ZERO, None)
        .await
        .unwrap();
    assert_eq!(deliveries.len(), 2);
    assert_eq!(&*deliveries[0].body, &[1; 20]);
    assert_eq!(&*deliveries[1].body, &[2; 20]);
}

#[tokio::test]
async fn rate_limited_scrub_does_not_hold_the_topic_lock() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        max_segment_bytes: 80 * 1024,
        max_message_bytes: 70 * 1024,
        scrub_bytes_per_second: 64 * 1024,
        ..BrokerConfig::default()
    })
    .unwrap();
    broker
        .publish("events", vec![vec![1; 64 * 1024]], Duration::ZERO)
        .await
        .unwrap();
    broker
        .publish("events", vec![vec![2; 32 * 1024]], Duration::ZERO)
        .await
        .unwrap();

    let scrub = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.scrub().await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    tokio::time::timeout(
        Duration::from_millis(300),
        broker.publish("events", vec![b"not-blocked".to_vec()], Duration::ZERO),
    )
    .await
    .expect("publish must not wait for the scrub I/O")
    .unwrap();
    assert!(scrub.await.unwrap().unwrap() > 0);
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
async fn concurrent_fin_and_req_share_a_durable_group_commit() {
    let root = tempdir().unwrap();
    let config = BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    };
    let broker = Broker::open(config.clone()).unwrap();
    broker.create_channel("events", "workers").await.unwrap();
    let bodies: Vec<_> = (0..16)
        .map(|ordinal| format!("message-{ordinal}").into_bytes())
        .collect();
    broker
        .publish("events", bodies, Duration::ZERO)
        .await
        .unwrap();
    let deliveries = broker
        .fetch_batch("events", "workers", 16, usize::MAX, Duration::ZERO, None)
        .await
        .unwrap();
    assert_eq!(deliveries.len(), 16);
    let requeued: HashSet<_> = deliveries
        .iter()
        .enumerate()
        .filter_map(|(ordinal, delivery)| (ordinal % 2 == 1).then_some(delivery.id))
        .collect();
    let commands = deliveries
        .into_iter()
        .enumerate()
        .map(|(ordinal, delivery)| {
            let broker = broker.clone();
            async move {
                if ordinal % 2 == 0 {
                    broker.finish("events", "workers", delivery.id).await
                } else {
                    broker
                        .requeue("events", "workers", delivery.id, Duration::ZERO)
                        .await
                }
            }
        });
    for result in join_all(commands).await {
        result.unwrap();
    }
    let commit = broker.stats().channel_group_commit;
    assert_eq!(commit.requests, 16);
    assert!(commit.commits < commit.requests);
    assert!(commit.max_batch_requests > 1);
    drop(broker);

    let broker = Broker::open(config).unwrap();
    let recovered = broker
        .fetch_batch("events", "workers", 16, usize::MAX, Duration::ZERO, None)
        .await
        .unwrap();
    assert_eq!(recovered.len(), 8);
    assert_eq!(
        recovered
            .into_iter()
            .map(|delivery| delivery.id)
            .collect::<HashSet<_>>(),
        requeued
    );
}

#[tokio::test]
async fn idle_publish_workers_retire_and_capacity_is_reusable() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        max_publish_workers: 1,
        publish_worker_idle: Duration::from_millis(20),
        ..BrokerConfig::default()
    })
    .unwrap();
    broker
        .publish("first", vec![b"one".to_vec()], Duration::ZERO)
        .await
        .unwrap();
    assert!(matches!(
        broker
            .publish("second", vec![b"two".to_vec()], Duration::ZERO)
            .await,
        Err(BrokerError::PublishWorkerLimit)
    ));
    tokio::time::sleep(Duration::from_millis(100)).await;
    broker
        .publish("second", vec![b"two".to_vec()], Duration::ZERO)
        .await
        .unwrap();
    let stats = broker.stats().publish_group_commit;
    assert!(stats.retired_workers >= 1);
    assert_eq!(stats.rejected_workers, 1);
    assert!(stats.active_workers <= 1);
}

#[tokio::test]
async fn broker_rejects_unbounded_durable_topic_creation() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        max_topics: 1,
        max_publish_workers: 2,
        ..BrokerConfig::default()
    })
    .unwrap();
    broker
        .publish("first", vec![b"one".to_vec()], Duration::ZERO)
        .await
        .unwrap();
    assert!(matches!(
        broker
            .publish("second", vec![b"two".to_vec()], Duration::ZERO)
            .await,
        Err(BrokerError::TopicLimit)
    ));
    assert_eq!(broker.topic_names(), vec!["first"]);
}

#[tokio::test]
async fn sealed_backlog_uses_bounded_metadata_residency() {
    let root = tempdir().unwrap();
    let config = BrokerConfig {
        data_path: root.path().into(),
        max_segment_bytes: 512,
        message_index_cache_bytes: 2 * 1024 * 60,
        bootstrap_retention: Duration::from_nanos(1),
        ..BrokerConfig::default()
    };
    let broker = Broker::open(config.clone()).unwrap();
    broker.create_channel("events", "workers").await.unwrap();
    for _ in 0..100 {
        broker
            .publish("events", vec![vec![0x5a; 64]; 32], Duration::ZERO)
            .await
            .unwrap();
    }
    broker.create_channel("events", "tail").await.unwrap();
    broker
        .publish("events", vec![vec![0x6b; 64]; 64], Duration::ZERO)
        .await
        .unwrap();
    broker.compact().await.unwrap();
    let handle = broker.topic("events").unwrap();
    let (active, sealed) = handle.state.lock().index_residency();
    assert_eq!(broker.stats().topics[0].message_count, 3_264);
    assert!(sealed >= 100);
    assert!(
        active <= 64,
        "only the current publish batch remains in active metadata"
    );
    drop(broker);

    let broker = Broker::open(config).unwrap();
    let handle = broker.topic("events").unwrap();
    let (active, sealed) = handle.state.lock().index_residency();
    assert_eq!(broker.stats().topics[0].message_count, 3_264);
    assert!(sealed >= 100);
    assert!(active <= 64);
    let deliveries = broker
        .fetch_batch("events", "workers", 64, usize::MAX, Duration::ZERO, None)
        .await
        .unwrap();
    assert_eq!(deliveries.len(), 64);
    let tail = broker
        .fetch_batch("events", "tail", 64, usize::MAX, Duration::ZERO, None)
        .await
        .unwrap();
    assert_eq!(tail.len(), 64);
    assert!(tail
        .iter()
        .all(|message| message.body.as_ref() == [0x6b; 64]));
    assert!(broker.inner.message_index_cache.resident_bytes() <= 2 * 1024 * 60);
}

#[tokio::test]
async fn idle_gc_does_not_seal_a_channel_blocked_active_segment() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        bootstrap_retention: Duration::from_nanos(1),
        ..BrokerConfig::default()
    })
    .unwrap();
    broker.create_channel("events", "blocked").await.unwrap();
    broker
        .publish("events", vec![b"retained".to_vec()], Duration::ZERO)
        .await
        .unwrap();

    for _ in 0..4 {
        assert_eq!(broker.compact().await.unwrap(), 0);
    }
    let stats = broker.stats();
    assert_eq!(stats.topics[0].segment_count, 1);
    assert_eq!(stats.topics[0].message_count, 1);
}

#[tokio::test]
async fn bounded_gc_rotates_across_topics() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        bootstrap_retention: Duration::from_nanos(1),
        ..BrokerConfig::default()
    })
    .unwrap();
    for topic in ["alpha", "beta", "gamma"] {
        broker
            .publish(topic, vec![b"expired".to_vec()], Duration::ZERO)
            .await
            .unwrap();
    }

    assert_eq!(broker.compact_some(1).await.unwrap(), 1);
    assert_eq!(
        broker
            .stats()
            .topics
            .iter()
            .map(|topic| topic.message_count)
            .sum::<u64>(),
        2
    );
    assert_eq!(broker.compact_some(1).await.unwrap(), 1);
    assert_eq!(
        broker
            .stats()
            .topics
            .iter()
            .map(|topic| topic.message_count)
            .sum::<u64>(),
        1
    );
}

#[tokio::test]
async fn delivery_budget_bounds_slow_consumers_and_cancellation_releases_reservations() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        max_message_bytes: 32,
        delivery_inflight_bytes: 64,
        ..BrokerConfig::default()
    })
    .unwrap();
    broker.create_channel("events", "alpha").await.unwrap();
    broker.create_channel("events", "beta").await.unwrap();
    broker
        .publish("events", vec![vec![0x5a; 32]], Duration::ZERO)
        .await
        .unwrap();

    let alpha = broker
        .fetch_batch_retained("events", "alpha", 1, 32, Duration::ZERO, None)
        .await
        .unwrap();
    let (alpha_messages, alpha_hold) = alpha.into_parts();
    assert_eq!(alpha_messages.len(), 1);
    assert_eq!(broker.stats().delivery_budget.in_flight_bytes, 64);

    let blocked = tokio::time::timeout(
        Duration::from_millis(20),
        broker.fetch_batch_retained("events", "beta", 1, 32, Duration::ZERO, None),
    )
    .await;
    assert!(blocked.is_err());
    assert_eq!(broker.stats().delivery_budget.waiters, 0);
    assert!(broker.stats().delivery_budget.waits_total >= 1);

    drop(alpha_messages);
    drop(alpha_hold);
    let beta = broker
        .fetch_batch_retained("events", "beta", 1, 32, Duration::ZERO, None)
        .await
        .unwrap();
    let (beta_messages, beta_hold) = beta.into_parts();
    assert_eq!(beta_messages.len(), 1);
    drop(beta_messages);
    drop(beta_hold);
    assert_eq!(broker.stats().delivery_budget.in_flight_bytes, 0);
}

#[tokio::test]
async fn dropping_an_unhanded_delivery_batch_does_not_consume_an_attempt() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    })
    .unwrap();
    broker.create_channel("events", "workers").await.unwrap();
    broker
        .publish("events", vec![b"payload".to_vec()], Duration::ZERO)
        .await
        .unwrap();

    let batch = broker
        .fetch_batch_retained("events", "workers", 1, 1024, Duration::ZERO, None)
        .await
        .unwrap();
    let (messages, guard) = batch.into_parts();
    assert_eq!(messages[0].attempts, 1);
    drop(messages);
    drop(guard);

    let messages = broker
        .fetch_batch("events", "workers", 1, 1024, Duration::ZERO, None)
        .await
        .unwrap();
    assert_eq!(messages[0].attempts, 1);
}

#[tokio::test]
async fn broker_metadata_budget_spills_active_tails_across_topics() {
    let root = tempdir().unwrap();
    let config = BrokerConfig {
        data_path: root.path().into(),
        message_index_cache_bytes: 64 * 1024,
        max_publish_workers: 128,
        ..BrokerConfig::default()
    };
    let broker = Broker::open(config.clone()).unwrap();
    broker.create_channel("topic-0", "workers").await.unwrap();
    for ordinal in 0..100 {
        broker
            .publish(
                &format!("topic-{ordinal}"),
                vec![vec![ordinal as u8; 8]; 32],
                Duration::ZERO,
            )
            .await
            .unwrap();
        if ordinal == 1 {
            let deliveries = broker
                .fetch_batch("topic-0", "workers", 32, usize::MAX, Duration::ZERO, None)
                .await
                .unwrap();
            assert_eq!(deliveries.len(), 32);
        }
    }
    assert_eq!(
        broker
            .stats()
            .topics
            .iter()
            .map(|topic| topic.message_count)
            .sum::<u64>(),
        3_200
    );
    assert!(broker.inner.message_index_cache.resident_bytes() <= 64 * 1024);
    drop(broker);

    let broker = Broker::open(config).unwrap();
    assert_eq!(
        broker
            .stats()
            .topics
            .iter()
            .map(|topic| topic.message_count)
            .sum::<u64>(),
        3_200
    );
    assert!(broker.inner.message_index_cache.resident_bytes() <= 64 * 1024);
}

#[tokio::test]
async fn concurrent_publishes_wait_for_metadata_spill_instead_of_rejecting() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        message_index_cache_bytes: 64 * 1024,
        max_publish_workers: 64,
        ..BrokerConfig::default()
    })
    .unwrap();
    let mut publishes = tokio::task::JoinSet::new();
    for ordinal in 0..32 {
        let broker = broker.clone();
        publishes.spawn(async move {
            broker
                .publish(
                    &format!("concurrent-{ordinal}"),
                    vec![vec![ordinal as u8; 8]; 1_024],
                    Duration::ZERO,
                )
                .await
        });
    }
    while let Some(result) = publishes.join_next().await {
        assert_eq!(result.unwrap().unwrap().len(), 1_024);
    }
    assert_eq!(
        broker
            .stats()
            .topics
            .iter()
            .map(|topic| topic.message_count)
            .sum::<u64>(),
        32 * 1_024
    );
    assert!(broker.inner.message_index_cache.resident_bytes() <= 64 * 1024);
}

#[tokio::test]
async fn management_fences_fail_closed_and_survive_restart() {
    let root = tempdir().unwrap();
    let config = BrokerConfig {
        data_path: root.path().into(),
        require_management_fence_sync: true,
        ..BrokerConfig::default()
    };
    let broker = Broker::open(config.clone()).unwrap();
    assert!(matches!(
        broker
            .publish("events", vec![b"blocked".to_vec()], Duration::ZERO)
            .await,
        Err(BrokerError::ManagementUnavailable)
    ));

    broker
        .sync_management_fences(ManagementFenceSnapshot::default())
        .await
        .unwrap();
    broker
        .publish("events", vec![b"accepted".to_vec()], Duration::ZERO)
        .await
        .unwrap();
    let deadline = now_ms() + 60_000;
    broker
        .manage_topic(
            "delete-events-0001",
            "events",
            TopicManagementAction::Delete,
            broker.registry_revision(),
            Some(deadline),
        )
        .await
        .unwrap();
    assert!(matches!(
        broker
            .publish("events", vec![b"blocked".to_vec()], Duration::ZERO)
            .await,
        Err(BrokerError::TopicTombstoned)
    ));
    drop(broker);

    let broker = Broker::open(config).unwrap();
    assert!(matches!(
        broker
            .publish("events", vec![b"blocked".to_vec()], Duration::ZERO)
            .await,
        Err(BrokerError::ManagementUnavailable)
    ));
    broker
        .sync_management_fences(ManagementFenceSnapshot {
            revision: "resource-version-2".into(),
            topics: BTreeMap::from([("events".into(), deadline)]),
            channels: Vec::new(),
        })
        .await
        .unwrap();
    assert!(matches!(
        broker
            .publish("events", vec![b"blocked".to_vec()], Duration::ZERO)
            .await,
        Err(BrokerError::TopicTombstoned)
    ));
    broker
        .manage_topic(
            "recreate-events-0001",
            "events",
            TopicManagementAction::Create,
            broker.registry_revision(),
            None,
        )
        .await
        .unwrap();
    broker
        .publish("events", vec![b"recreated".to_vec()], Duration::ZERO)
        .await
        .unwrap();
}

#[tokio::test]
async fn management_rejects_stale_registry_revisions() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    })
    .unwrap();
    let stale = broker.registry_revision();
    let created = broker
        .manage_topic(
            "create-events-0001",
            "events",
            TopicManagementAction::Create,
            stale,
            None,
        )
        .await
        .unwrap();
    assert!(created.revision > stale);
    let error = broker
        .manage_topic(
            "pause-events-00001",
            "events",
            TopicManagementAction::Pause,
            stale,
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        BrokerError::RevisionConflict {
            expected,
            actual
        } if expected == stale && actual == created.revision
    ));
    assert!(!broker.stats().topics[0].paused);
}

#[tokio::test]
async fn management_operation_results_are_idempotent_across_restart() {
    let root = tempdir().unwrap();
    let config = BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    };
    let broker = Broker::open(config.clone()).unwrap();
    let expected = broker.registry_revision();
    let first = broker
        .manage_topic(
            "create-orders-0001",
            "orders",
            TopicManagementAction::Create,
            expected,
            None,
        )
        .await
        .unwrap();
    let duplicate = broker
        .manage_topic(
            "create-orders-0001",
            "orders",
            TopicManagementAction::Create,
            expected,
            None,
        )
        .await
        .unwrap();
    assert_eq!(duplicate, first);
    drop(broker);

    let broker = Broker::open(config).unwrap();
    let replay = broker
        .manage_topic(
            "create-orders-0001",
            "orders",
            TopicManagementAction::Create,
            0,
            None,
        )
        .await
        .unwrap();
    assert_eq!(replay, first);
    assert!(matches!(
        broker
            .manage_topic(
                "create-orders-0001",
                "orders",
                TopicManagementAction::Pause,
                broker.registry_revision(),
                None,
            )
            .await,
        Err(BrokerError::OperationConflict)
    ));
    assert!(broker.storage_healthy());
}

#[tokio::test]
async fn concurrent_registry_updates_persist_the_latest_revision() {
    let root = tempdir().unwrap();
    let config = BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    };
    let broker = Broker::open(config.clone()).unwrap();
    let mut tasks = Vec::new();
    for ordinal in 0..64 {
        let broker = broker.clone();
        tasks.push(tokio::spawn(async move {
            broker
                .create_channel(&format!("topic-{ordinal}"), "workers")
                .await
                .unwrap();
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let expected = broker.registry_revision();
    drop(broker);

    let reopened = Broker::open(config).unwrap();
    assert_eq!(reopened.registry_revision(), expected);
}

#[tokio::test]
async fn panicked_storage_tasks_fail_the_broker_closed() {
    let error = super::blocking::<()>(|| panic!("injected storage task panic"))
        .await
        .unwrap_err();
    assert!(matches!(error, BrokerError::StorageUnavailable));
}
