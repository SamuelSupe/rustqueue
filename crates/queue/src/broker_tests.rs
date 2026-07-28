use super::io::SEQUENCE_RESERVATION;
use super::*;
use crate::outbox::OutboxEntry;
use crate::{
    ChannelManagementAction, ChannelManagementCommand, ManagementFenceSnapshot,
    TopicManagementAction,
};
use futures::future::join_all;
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tempfile::tempdir;

struct DropProbe(Arc<AtomicBool>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_publish_keeps_admission_guard_until_commit_finishes() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    })
    .unwrap();
    broker.create_topic("events").await.unwrap();

    let topic = broker.topic("events").unwrap();
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let lock_thread = std::thread::spawn(move || {
        let _lock = topic.state.lock();
        locked_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    locked_rx.recv().unwrap();

    let dropped = Arc::new(AtomicBool::new(false));
    let publish = {
        let broker = broker.clone();
        let dropped = Arc::clone(&dropped);
        tokio::spawn(async move {
            broker
                .publish_guarded(
                    "events",
                    vec![bytes::Bytes::from_static(b"body")],
                    Duration::ZERO,
                    DropProbe(dropped),
                )
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        while broker.inner.publish_groups.stats().active_workers == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    publish.abort();
    assert!(publish.await.unwrap_err().is_cancelled());
    assert!(!dropped.load(Ordering::Acquire));

    release_tx.send(()).unwrap();
    lock_thread.join().unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !dropped.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

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
async fn blocked_dlq_target_does_not_prevent_broker_restart() {
    let root = tempdir().unwrap();
    let config = BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    };
    let broker = Broker::open(config.clone()).unwrap();
    broker.create_channel("events", "workers").await.unwrap();
    let id = broker
        .publish("events", vec![b"poison".to_vec()], Duration::ZERO)
        .await
        .unwrap()[0];
    let target = "events.workers.DLQ";
    crate::outbox::store(
        &root.path().join("dlq-outbox"),
        &OutboxEntry {
            source_topic: "events".into(),
            source_channel: "workers".into(),
            message_id: id,
            target_topic: target.into(),
            body: bytes::Bytes::from_static(b"poison"),
        },
    )
    .unwrap();
    broker
        .sync_management_fences(ManagementFenceSnapshot {
            revision: "blocked-dlq-target".into(),
            topics: BTreeMap::from([(target.into(), now_ms() + 60_000)]),
            channels: Vec::new(),
        })
        .await
        .unwrap();
    drop(broker);

    let broker = Broker::open(config).unwrap();
    assert_eq!(
        std::fs::read_dir(root.path().join("dlq-outbox"))
            .unwrap()
            .count(),
        1
    );
    let stats = broker.stats();
    let source = stats
        .topics
        .iter()
        .find(|topic| topic.name == "events")
        .unwrap();
    assert_eq!(source.channels[0].depth, 1);
    assert!(!stats.topics.iter().any(|topic| topic.name == target));
}

#[tokio::test]
async fn startup_does_not_replay_outbox_before_required_fence_sync() {
    let root = tempdir().unwrap();
    let config = BrokerConfig {
        data_path: root.path().into(),
        require_management_fence_sync: true,
        ..BrokerConfig::default()
    };
    let broker = Broker::open(config.clone()).unwrap();
    broker
        .sync_management_fences(ManagementFenceSnapshot::default())
        .await
        .unwrap();
    broker.create_channel("events", "workers").await.unwrap();
    let id = broker
        .publish("events", vec![b"poison".to_vec()], Duration::ZERO)
        .await
        .unwrap()[0];
    let target = "events.workers.DLQ";
    crate::outbox::store(
        &root.path().join("dlq-outbox"),
        &OutboxEntry {
            source_topic: "events".into(),
            source_channel: "workers".into(),
            message_id: id,
            target_topic: target.into(),
            body: bytes::Bytes::from_static(b"poison"),
        },
    )
    .unwrap();
    drop(broker);

    let broker = Broker::open(config).unwrap();
    assert!(!broker.management_fences_ready());
    assert_eq!(
        std::fs::read_dir(root.path().join("dlq-outbox"))
            .unwrap()
            .count(),
        1
    );
    let stats = broker.stats();
    let source = stats
        .topics
        .iter()
        .find(|topic| topic.name == "events")
        .unwrap();
    assert_eq!(source.channels[0].depth, 1);
    assert!(!stats.topics.iter().any(|topic| topic.name == target));
}

#[tokio::test]
async fn concurrent_dlq_moves_publish_only_one_copy() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    })
    .unwrap();
    broker.create_channel("events", "workers").await.unwrap();
    let id = broker
        .publish("events", vec![b"poison".to_vec()], Duration::ZERO)
        .await
        .unwrap()[0];

    let first = broker.move_to_dead_letter(
        "events",
        "workers",
        id,
        "events.workers.DLQ",
        bytes::Bytes::from_static(b"poison"),
    );
    let second = broker.move_to_dead_letter(
        "events",
        "workers",
        id,
        "events.workers.DLQ",
        bytes::Bytes::from_static(b"poison"),
    );
    let (first, second) = tokio::join!(first, second);
    assert_eq!(
        [first.unwrap(), second.unwrap()]
            .into_iter()
            .filter(|moved| *moved)
            .count(),
        1
    );

    let stats = broker.stats();
    let source = stats
        .topics
        .iter()
        .find(|topic| topic.name == "events")
        .unwrap();
    assert_eq!(source.channels[0].depth, 0);
    let target = stats
        .topics
        .iter()
        .find(|topic| topic.name == "events.workers.DLQ")
        .unwrap();
    assert_eq!(target.message_count, 1);
}

#[tokio::test]
async fn cancelled_dlq_move_keeps_the_transaction_serialized_until_completion() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    })
    .unwrap();
    broker.create_channel("events", "workers").await.unwrap();
    let id = broker
        .publish("events", vec![b"poison".to_vec()], Duration::ZERO)
        .await
        .unwrap()[0];

    let blocker = broker.inner.outbox_moves.lock().await;
    let mut moving = Box::pin(broker.move_to_dead_letter(
        "events",
        "workers",
        id,
        "events.workers.DLQ",
        bytes::Bytes::from_static(b"poison"),
    ));
    std::future::poll_fn(|context| {
        assert!(
            std::future::Future::poll(moving.as_mut(), context).is_pending(),
            "the blocked move must not complete"
        );
        std::task::Poll::Ready(())
    })
    .await;
    drop(moving);
    drop(blocker);

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let stats = broker.stats();
            let source_depth = stats
                .topics
                .iter()
                .find(|topic| topic.name == "events")
                .map(|topic| topic.channels[0].depth);
            let target_count = stats
                .topics
                .iter()
                .find(|topic| topic.name == "events.workers.DLQ")
                .map(|topic| topic.message_count);
            let outbox_empty = std::fs::read_dir(root.path().join("dlq-outbox"))
                .is_ok_and(|entries| entries.count() == 0);
            if source_depth == Some(0) && target_count == Some(1) && outbox_empty {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    assert!(!broker
        .move_to_dead_letter(
            "events",
            "workers",
            id,
            "events.workers.DLQ",
            bytes::Bytes::from_static(b"poison"),
        )
        .await
        .unwrap());
    assert_eq!(
        broker
            .stats()
            .topics
            .iter()
            .find(|topic| topic.name == "events.workers.DLQ")
            .unwrap()
            .message_count,
        1
    );
}

#[tokio::test]
async fn completed_dlq_outbox_is_not_published_again_after_restart() {
    let root = tempdir().unwrap();
    let config = BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    };
    let broker = Broker::open(config.clone()).unwrap();
    broker.create_channel("events", "workers").await.unwrap();
    let id = broker
        .publish("events", vec![b"poison".to_vec()], Duration::ZERO)
        .await
        .unwrap()[0];
    let target = "events.workers.DLQ";
    let outbox_path = crate::outbox::store(
        &root.path().join("dlq-outbox"),
        &OutboxEntry {
            source_topic: "events".into(),
            source_channel: "workers".into(),
            message_id: id,
            target_topic: target.into(),
            body: bytes::Bytes::from_static(b"poison"),
        },
    )
    .unwrap();
    broker
        .publish(target, vec![b"poison".to_vec()], Duration::ZERO)
        .await
        .unwrap();
    let delivery = broker
        .next_message("events", "workers", None)
        .await
        .unwrap()
        .unwrap();
    broker
        .finish("events", "workers", delivery.id)
        .await
        .unwrap();
    assert!(outbox_path.exists());
    drop(broker);

    let broker = Broker::open(config).unwrap();
    assert!(!outbox_path.exists());
    let target = broker
        .stats()
        .topics
        .into_iter()
        .find(|topic| topic.name == target)
        .unwrap();
    assert_eq!(target.message_count, 1);
}

#[tokio::test]
async fn dlq_outbox_recovers_when_the_source_was_removed() {
    let root = tempdir().unwrap();
    let config = BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    };
    let broker = Broker::open(config.clone()).unwrap();
    broker.create_channel("events", "workers").await.unwrap();
    let id = broker
        .publish("events", vec![b"poison".to_vec()], Duration::ZERO)
        .await
        .unwrap()[0];
    let target = "events.workers.DLQ";
    crate::outbox::store(
        &root.path().join("dlq-outbox"),
        &OutboxEntry {
            source_topic: "events".into(),
            source_channel: "workers".into(),
            message_id: id,
            target_topic: target.into(),
            body: bytes::Bytes::from_static(b"poison"),
        },
    )
    .unwrap();
    broker.delete_topic("events").await.unwrap();
    drop(broker);

    let broker = Broker::open(config).unwrap();
    assert_eq!(
        std::fs::read_dir(root.path().join("dlq-outbox"))
            .unwrap()
            .count(),
        0
    );
    let target = broker
        .stats()
        .topics
        .into_iter()
        .find(|topic| topic.name == target)
        .unwrap();
    assert_eq!(target.message_count, 1);
}

#[tokio::test]
async fn stats_settle_expired_in_flight_messages_without_another_fetch() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    })
    .unwrap();
    broker.create_channel("events", "workers").await.unwrap();
    broker
        .publish("events", vec![b"body".to_vec()], Duration::ZERO)
        .await
        .unwrap();
    assert!(broker
        .next_message("events", "workers", Some(Duration::ZERO))
        .await
        .unwrap()
        .is_some());

    broker.expire_in_flight().await.unwrap();
    let stats = broker.stats();
    let channel = &stats.topics[0].channels[0];
    assert_eq!(channel.in_flight_count, 0);
    assert_eq!(channel.timeout_count, 1);
    assert_eq!(channel.depth, 1);
}

#[tokio::test]
async fn stale_delivery_token_cannot_mutate_a_redelivery() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    })
    .unwrap();
    broker.create_channel("events", "workers").await.unwrap();
    broker
        .publish("events", vec![b"body".to_vec()], Duration::ZERO)
        .await
        .unwrap();

    let first = broker
        .fetch_batch_retained(
            "events",
            "workers",
            1,
            usize::MAX,
            Duration::ZERO,
            Some(Duration::ZERO),
        )
        .await
        .unwrap();
    let (first, mut first_guard) = first.into_parts();
    let id = first[0].id;
    let stale_token = first_guard.accept_with_token(id).unwrap();
    broker
        .expire_channel_in_flight("events", "workers")
        .await
        .unwrap();

    let redelivery = broker
        .fetch_batch_retained("events", "workers", 1, usize::MAX, Duration::ZERO, None)
        .await
        .unwrap();
    let (redelivery, mut redelivery_guard) = redelivery.into_parts();
    assert_eq!(redelivery[0].id, id);
    let current_token = redelivery_guard.accept_with_token(id).unwrap();
    assert_ne!(stale_token, current_token);

    assert!(matches!(
        broker
            .finish_delivery("events", "workers", id, stale_token)
            .await,
        Err(BrokerError::MessageNotInFlight)
    ));
    assert!(matches!(
        broker.touch_delivery("events", "workers", id, stale_token, None),
        Err(BrokerError::MessageNotInFlight)
    ));
    assert!(matches!(
        broker
            .requeue_delivery("events", "workers", id, stale_token, Duration::ZERO)
            .await,
        Err(BrokerError::MessageNotInFlight)
    ));
    broker.release_deliveries("events", "workers", &[(id, stale_token)]);
    assert_eq!(broker.stats().topics[0].channels[0].in_flight_count, 1);

    broker
        .finish_delivery("events", "workers", id, current_token)
        .await
        .unwrap();
    assert_eq!(broker.stats().topics[0].channels[0].depth, 0);
}

#[tokio::test]
async fn current_delivery_tokens_can_renew_an_expiring_batch() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    })
    .unwrap();
    broker.create_channel("events", "workers").await.unwrap();
    broker
        .publish(
            "events",
            vec![b"first".to_vec(), b"second".to_vec()],
            Duration::ZERO,
        )
        .await
        .unwrap();

    let batch = broker
        .fetch_batch_retained(
            "events",
            "workers",
            2,
            usize::MAX,
            Duration::ZERO,
            Some(Duration::ZERO),
        )
        .await
        .unwrap();
    let (deliveries, mut guard) = batch.into_parts();
    let tokens: Vec<_> = deliveries
        .iter()
        .map(|delivery| (delivery.id, guard.token(delivery.id).unwrap()))
        .collect();
    broker
        .touch_deliveries("events", "workers", &tokens, Some(Duration::from_secs(30)))
        .unwrap();
    for (id, token) in &tokens {
        assert_eq!(guard.accept_with_token(*id), Some(*token));
    }

    broker
        .expire_channel_in_flight("events", "workers")
        .await
        .unwrap();
    assert_eq!(broker.stats().topics[0].channels[0].in_flight_count, 2);
    for (id, token) in tokens {
        broker
            .finish_delivery("events", "workers", id, token)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn kodo_channel_counters_are_monotonic_across_empty_and_restart() {
    if rustqueue_storage::MAX_WRITER_FEATURE_LEVEL < 2 {
        return;
    }
    let root = tempdir().unwrap();
    let config = BrokerConfig {
        data_path: root.path().into(),
        storage_feature_level: 2,
        ..BrokerConfig::default()
    };
    let broker = Broker::open(config.clone()).unwrap();
    broker.create_channel("events", "workers").await.unwrap();
    broker
        .publish(
            "events",
            vec![b"one".to_vec(), b"two".to_vec()],
            Duration::ZERO,
        )
        .await
        .unwrap();
    let delivery = broker
        .next_message("events", "workers", None)
        .await
        .unwrap()
        .unwrap();
    broker
        .requeue("events", "workers", delivery.id, Duration::ZERO)
        .await
        .unwrap();
    assert!(broker
        .next_message("events", "workers", Some(Duration::ZERO))
        .await
        .unwrap()
        .is_some());
    broker.expire_in_flight().await.unwrap();
    broker.empty_channel("events", "workers").await.unwrap();
    broker
        .publish("events", vec![b"three".to_vec()], Duration::ZERO)
        .await
        .unwrap();

    let channel = &broker.stats().topics[0].channels[0];
    assert_eq!(channel.message_count, 3);
    assert_eq!(channel.requeue_count, 1);
    assert_eq!(channel.timeout_count, 1);
    drop(broker);

    let reopened = Broker::open(config).unwrap();
    let channel = &reopened.stats().topics[0].channels[0];
    assert_eq!(channel.message_count, 3);
    assert_eq!(channel.requeue_count, 1);
    assert_eq!(channel.timeout_count, 1);
}

#[tokio::test]
async fn durable_batch_accepts_the_full_protocol_message_count() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    })
    .unwrap();
    let messages = vec![vec![b'x']; rustqueue_protocol::MAX_MPUB_MESSAGES];
    let ids = broker
        .publish("events", messages, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(ids.len(), rustqueue_protocol::MAX_MPUB_MESSAGES);
    assert_eq!(
        broker.stats().topics[0].published_count,
        rustqueue_protocol::MAX_MPUB_MESSAGES as u64
    );
}

#[tokio::test]
async fn startup_replays_a_large_dlq_outbox_after_lowering_the_publish_limit() {
    if rustqueue_storage::MAX_WRITER_FEATURE_LEVEL < 2 {
        return;
    }
    let root = tempdir().unwrap();
    let original = BrokerConfig {
        data_path: root.path().into(),
        max_segment_bytes: 256 * 1024 * 1024,
        max_message_bytes: 100 * 1024 * 1024,
        storage_feature_level: 2,
        ..BrokerConfig::default()
    };
    let broker = Broker::open(original.clone()).unwrap();
    let body = bytes::Bytes::from(vec![0x6b; 100 * 1024 * 1024]);
    let entry = OutboxEntry {
        source_topic: "missing-source".into(),
        source_channel: "missing-channel".into(),
        message_id: 1,
        target_topic: "events.DLQ".into(),
        body: body.clone(),
    };
    crate::outbox::store(&root.path().join("dlq-outbox"), &entry).unwrap();
    drop(broker);

    let broker = Broker::open(BrokerConfig {
        max_message_bytes: 20 * 1024 * 1024,
        ..original
    })
    .unwrap();
    broker
        .create_channel("events.DLQ", "inspect")
        .await
        .unwrap();
    let delivery = broker
        .next_message("events.DLQ", "inspect", None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivery.body.as_ref(), body.as_ref());
    assert_eq!(
        std::fs::read_dir(root.path().join("dlq-outbox"))
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn feature_level_two_rejects_a_delivery_budget_that_cannot_read_retained_messages() {
    let root = tempdir().unwrap();
    let error = match Broker::open(BrokerConfig {
        data_path: root.path().into(),
        max_message_bytes: 20 * 1024 * 1024,
        delivery_inflight_bytes: 40 * 1024 * 1024,
        storage_feature_level: 2,
        ..BrokerConfig::default()
    }) {
        Ok(_) => panic!("undersized delivery budget was accepted"),
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("every message readable at the active storage feature level"));
}

#[test]
fn broker_rejects_timeouts_that_would_overflow_instant() {
    let root = tempdir().unwrap();
    let error = match Broker::open(BrokerConfig {
        data_path: root.path().into(),
        message_timeout: Duration::MAX,
        ..BrokerConfig::default()
    }) {
        Ok(_) => panic!("unrepresentable broker timeout was accepted"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("platform timer range"));
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
async fn deferred_stats_are_exact_without_a_consumer_and_after_restart() {
    let root = tempdir().unwrap();
    let config = BrokerConfig {
        data_path: root.path().into(),
        max_ack_gap: 2,
        ..BrokerConfig::default()
    };
    let broker = Broker::open(config.clone()).unwrap();
    broker.create_channel("events", "workers").await.unwrap();
    broker
        .publish(
            "events",
            vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()],
            Duration::ZERO,
        )
        .await
        .unwrap();
    broker
        .publish(
            "events",
            vec![b"deferred".to_vec()],
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    let channel = &broker.stats().topics[0].channels[0];
    assert_eq!((channel.depth, channel.deferred_count), (4, 1));
    drop(broker);

    let reopened = Broker::open(config).unwrap();
    let channel = &reopened.stats().topics[0].channels[0];
    assert_eq!((channel.depth, channel.deferred_count), (4, 1));
}

#[tokio::test]
async fn deferred_stats_promote_messages_when_the_deadline_passes() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    })
    .unwrap();
    broker.create_channel("events", "workers").await.unwrap();
    broker
        .publish(
            "events",
            vec![b"deferred".to_vec()],
            Duration::from_millis(20),
        )
        .await
        .unwrap();
    assert_eq!(broker.stats().topics[0].channels[0].deferred_count, 1);
    tokio::time::sleep(Duration::from_millis(40)).await;
    let channel = &broker.stats().topics[0].channels[0];
    assert_eq!((channel.depth, channel.deferred_count), (1, 0));
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
async fn maintenance_reclaims_retired_topics_after_readers_drain() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    })
    .unwrap();
    broker.create_topic("events").await.unwrap();
    let handle = broker.topic("events").unwrap();
    let directory = crate::metadata::topic_directory(root.path(), "events");

    broker.delete_topic("events").await.unwrap();
    assert!(directory.exists());
    assert!(broker.inner.retired_topics.lock().contains_key("events"));

    drop(handle);
    broker.compact().await.unwrap();

    assert!(!directory.exists());
    assert!(!broker.inner.retired_topics.lock().contains_key("events"));
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
async fn channel_creation_rechecks_fences_after_waiting_for_management() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    })
    .unwrap();
    broker.create_topic("events").await.unwrap();

    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let lock_holder = {
        let broker = broker.clone();
        std::thread::spawn(move || {
            let _lifecycle = broker.inner.topic_lifecycle.lock();
            locked_tx.send(()).unwrap();
            let _ = release_rx.recv();
        })
    };
    locked_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let mut creation = Box::pin(broker.create_channel("events", "workers"));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), creation.as_mut())
            .await
            .is_err(),
        "ordinary channel creation bypassed the management lifecycle barrier"
    );
    {
        let mut fences = broker.inner.fences.lock();
        fences.set_channel("events", "workers", now_ms() + 60_000);
        fences.store(&broker.inner.fences_path).unwrap();
    }
    release_tx.send(()).unwrap();
    lock_holder.join().unwrap();

    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), creation)
            .await
            .unwrap(),
        Err(BrokerError::ChannelTombstoned)
    ));
    assert!(broker.channel_names("events").unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn waiting_fetch_rechecks_channel_fence_before_reserving() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    })
    .unwrap();
    broker.create_channel("events", "workers").await.unwrap();

    let topic = broker.topic("events").unwrap();
    let baseline_receivers = topic.wake.receiver_count();
    let fetch_broker = broker.clone();
    let fetch = tokio::spawn(async move {
        fetch_broker
            .fetch_batch_retained("events", "workers", 1, 1024, Duration::from_secs(1), None)
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while topic.wake.receiver_count() <= baseline_receivers {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let topic_state = topic.state.lock();
    {
        let mut fences = broker.inner.fences.lock();
        fences.set_channel("events", "workers", now_ms() + 60_000);
        fences.store(&broker.inner.fences_path).unwrap();
    }
    drop(topic_state);
    broker
        .publish("events", vec![b"body".to_vec()], Duration::ZERO)
        .await
        .unwrap();

    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), fetch)
            .await
            .unwrap()
            .unwrap(),
        Err(BrokerError::ChannelTombstoned)
    ));
    let channel = &broker.stats().topics[0].channels[0];
    assert_eq!(channel.depth, 1);
    assert_eq!(channel.in_flight_count, 0);
}

#[tokio::test]
async fn touch_rechecks_channel_fence_after_waiting_for_topic() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    })
    .unwrap();
    broker.create_channel("events", "workers").await.unwrap();
    broker
        .publish("events", vec![b"body".to_vec()], Duration::ZERO)
        .await
        .unwrap();
    let batch = broker
        .fetch_batch_retained("events", "workers", 1, 1024, Duration::ZERO, None)
        .await
        .unwrap();
    let (deliveries, mut guard) = batch.into_parts();
    let id = deliveries[0].id;
    let token = guard.accept_with_token(id).unwrap();

    let topic = broker.topic("events").unwrap();
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let lock_holder = {
        let topic = Arc::clone(&topic);
        std::thread::spawn(move || {
            let _topic_state = topic.state.lock();
            locked_tx.send(()).unwrap();
            let _ = release_rx.recv();
        })
    };
    locked_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let baseline_references = Arc::strong_count(&topic);
    let toucher = {
        let broker = broker.clone();
        std::thread::spawn(move || {
            broker.touch_delivery(
                "events",
                "workers",
                id,
                token,
                Some(Duration::from_secs(30)),
            )
        })
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        while Arc::strong_count(&topic) <= baseline_references {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    {
        let mut fences = broker.inner.fences.lock();
        fences.set_channel("events", "workers", now_ms() + 60_000);
        fences.store(&broker.inner.fences_path).unwrap();
    }
    release_tx.send(()).unwrap();
    lock_holder.join().unwrap();

    assert!(matches!(
        toucher.join().unwrap(),
        Err(BrokerError::ChannelTombstoned)
    ));
}

#[tokio::test]
async fn fence_sync_waits_for_active_management_mutation() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    })
    .unwrap();
    let deadline = now_ms() + 60_000;

    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let lock_holder = {
        let broker = broker.clone();
        std::thread::spawn(move || {
            let _lifecycle = broker.inner.topic_lifecycle.lock();
            locked_tx.send(()).unwrap();
            let _ = release_rx.recv();
        })
    };
    locked_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let mut sync = Box::pin(broker.sync_management_fences(ManagementFenceSnapshot {
        revision: "resource-version-1".into(),
        topics: BTreeMap::from([("events".into(), deadline)]),
        channels: Vec::new(),
    }));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), sync.as_mut())
            .await
            .is_err(),
        "fence replacement bypassed the management lifecycle barrier"
    );
    release_tx.send(()).unwrap();
    lock_holder.join().unwrap();
    tokio::time::timeout(Duration::from_secs(1), sync)
        .await
        .unwrap()
        .unwrap();

    assert!(matches!(
        broker.create_topic("events").await,
        Err(BrokerError::TopicTombstoned)
    ));
}

#[tokio::test]
async fn failed_management_preconditions_do_not_leave_durable_blockers() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    })
    .unwrap();

    assert!(matches!(
        broker
            .manage_topic(
                "pause-missing-0001",
                "missing",
                TopicManagementAction::Pause,
                broker.registry_revision(),
                None,
            )
            .await,
        Err(BrokerError::TopicNotFound)
    ));
    broker.create_topic("missing").await.unwrap();

    broker.create_topic("events").await.unwrap();
    assert!(matches!(
        broker
            .manage_channel(ChannelManagementCommand {
                operation_id: "pause-channel-0001",
                topic: "events",
                channel: "workers",
                action: ChannelManagementAction::Pause,
                expected_revision: broker.registry_revision(),
                tombstone_until_ms: None,
                require_idle: false,
            })
            .await,
        Err(BrokerError::ChannelNotFound)
    ));
    broker.create_channel("events", "workers").await.unwrap();

    assert!(matches!(
        broker
            .manage_topic(
                "delete-no-deadline-0001",
                "missing",
                TopicManagementAction::Delete,
                broker.registry_revision(),
                None,
            )
            .await,
        Err(BrokerError::InvalidTombstone)
    ));
    broker
        .publish("missing", vec![b"still-open".to_vec()], Duration::ZERO)
        .await
        .unwrap();

    assert!(matches!(
        broker
            .manage_channel(ChannelManagementCommand {
                operation_id: "delete-expired-0001",
                topic: "events",
                channel: "workers",
                action: ChannelManagementAction::Delete,
                expected_revision: broker.registry_revision(),
                tombstone_until_ms: Some(now_ms().saturating_sub(1)),
                require_idle: false,
            })
            .await,
        Err(BrokerError::InvalidTombstone)
    ));
    broker
        .publish("events", vec![b"still-open".to_vec()], Duration::ZERO)
        .await
        .unwrap();
}

#[tokio::test]
async fn a_different_management_action_cannot_cross_a_pending_operation() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    })
    .unwrap();
    broker.create_topic("events").await.unwrap();
    let fingerprint =
        serde_json::to_string(&("topic", "events", TopicManagementAction::Pause)).unwrap();
    broker
        .inner
        .management_ops
        .lock()
        .prepare(
            &broker.inner.management_ops_path,
            "pending-pause-0001",
            fingerprint,
            "events".into(),
        )
        .unwrap();

    assert!(matches!(
        broker
            .manage_topic(
                "delete-events-0002",
                "events",
                TopicManagementAction::Delete,
                broker.registry_revision(),
                Some(now_ms() + 60_000),
            )
            .await,
        Err(BrokerError::OperationConflict)
    ));
    assert!(broker.topic_names().iter().any(|topic| topic == "events"));
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
async fn delete_if_idle_rejects_backlog_without_leaving_a_management_block() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    })
    .unwrap();
    broker.create_channel("events", "workers").await.unwrap();
    broker
        .publish("events", vec![b"one".to_vec()], Duration::ZERO)
        .await
        .unwrap();
    let revision = broker.registry_revision();
    let deadline = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .min(i64::MAX as u128) as i64
        + 60_000;
    let error = broker
        .manage_channel(ChannelManagementCommand {
            operation_id: "delete-idle-test-0001",
            topic: "events",
            channel: "workers",
            action: ChannelManagementAction::Delete,
            expected_revision: revision,
            tombstone_until_ms: Some(deadline),
            require_idle: true,
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        BrokerError::ChannelNotIdle {
            depth: 1,
            in_flight: 0,
            deferred: 0
        }
    ));

    broker
        .publish("events", vec![b"two".to_vec()], Duration::ZERO)
        .await
        .unwrap();
    broker.empty_channel("events", "workers").await.unwrap();
    broker
        .manage_channel(ChannelManagementCommand {
            operation_id: "delete-idle-test-0002",
            topic: "events",
            channel: "workers",
            action: ChannelManagementAction::Delete,
            expected_revision: broker.registry_revision(),
            tombstone_until_ms: Some(deadline),
            require_idle: true,
        })
        .await
        .unwrap();
    assert!(broker.channel_names("events").unwrap().is_empty());
}

#[tokio::test]
async fn delete_if_idle_reports_missing_topics_and_channels() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    })
    .unwrap();
    let deadline = now_ms() + 60_000;

    let missing_topic = broker
        .manage_channel(ChannelManagementCommand {
            operation_id: "delete-missing-topic-0001",
            topic: "events",
            channel: "workers",
            action: ChannelManagementAction::Delete,
            expected_revision: broker.registry_revision(),
            tombstone_until_ms: Some(deadline),
            require_idle: true,
        })
        .await
        .unwrap_err();
    assert!(matches!(missing_topic, BrokerError::TopicNotFound));

    broker.create_channel("events", "workers").await.unwrap();
    broker.empty_channel("events", "workers").await.unwrap();
    broker
        .manage_channel(ChannelManagementCommand {
            operation_id: "delete-existing-channel-0001",
            topic: "events",
            channel: "workers",
            action: ChannelManagementAction::Delete,
            expected_revision: broker.registry_revision(),
            tombstone_until_ms: Some(deadline),
            require_idle: true,
        })
        .await
        .unwrap();

    let missing_channel = broker
        .manage_channel(ChannelManagementCommand {
            operation_id: "delete-missing-channel-0001",
            topic: "events",
            channel: "workers",
            action: ChannelManagementAction::Delete,
            expected_revision: broker.registry_revision(),
            tombstone_until_ms: Some(deadline),
            require_idle: true,
        })
        .await
        .unwrap_err();
    assert!(matches!(missing_channel, BrokerError::ChannelNotFound));
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
async fn expired_pending_tombstone_can_finish_and_unblock_the_topic() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    })
    .unwrap();
    broker.create_topic("orders").await.unwrap();
    let operation_id = "tombstone-orders-0001";
    let fingerprint =
        serde_json::to_string(&("topic", "orders", TopicManagementAction::Tombstone)).unwrap();
    broker
        .inner
        .management_ops
        .lock()
        .prepare(
            &broker.inner.management_ops_path,
            operation_id,
            fingerprint,
            "orders".into(),
        )
        .unwrap();

    broker
        .manage_topic(
            operation_id,
            "orders",
            TopicManagementAction::Tombstone,
            0,
            Some(now_ms().saturating_sub(1)),
        )
        .await
        .unwrap();

    broker
        .publish("orders", vec![b"accepted".to_vec()], Duration::ZERO)
        .await
        .unwrap();
}

#[tokio::test]
async fn changed_cleanup_operation_id_adopts_a_pending_delete_after_restart() {
    let root = tempdir().unwrap();
    let config = BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    };
    let broker = Broker::open(config.clone()).unwrap();
    broker.create_channel("events", "workers").await.unwrap();
    let fingerprint = serde_json::to_string(&(
        "channel",
        "events",
        "workers",
        ChannelManagementAction::Delete,
        true,
    ))
    .unwrap();
    broker
        .inner
        .management_ops
        .lock()
        .prepare(
            &broker.inner.management_ops_path,
            "kodo-delete-old-0001",
            fingerprint,
            "events".into(),
        )
        .unwrap();
    drop(broker);

    let broker = Broker::open(config).unwrap();
    broker
        .manage_channel(ChannelManagementCommand {
            operation_id: "kodo-delete-new-0001",
            topic: "events",
            channel: "workers",
            action: ChannelManagementAction::Delete,
            expected_revision: 0,
            tombstone_until_ms: Some(now_ms() + 60_000),
            require_idle: true,
        })
        .await
        .unwrap();
    assert!(broker.channel_names("events").unwrap().is_empty());
    assert!(!broker.inner.management_ops.lock().blocks_topic("events"));
}

#[tokio::test]
async fn pending_idle_delete_completes_when_the_channel_was_already_removed() {
    let root = tempdir().unwrap();
    let config = BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    };
    let broker = Broker::open(config.clone()).unwrap();
    broker.create_channel("events", "workers").await.unwrap();
    let fingerprint = serde_json::to_string(&(
        "channel",
        "events",
        "workers",
        ChannelManagementAction::Delete,
        true,
    ))
    .unwrap();
    broker
        .inner
        .management_ops
        .lock()
        .prepare(
            &broker.inner.management_ops_path,
            "kodo-delete-old-0002",
            fingerprint,
            "events".into(),
        )
        .unwrap();
    broker
        .topic("events")
        .unwrap()
        .state
        .lock()
        .delete_channel("workers")
        .unwrap();
    drop(broker);

    let broker = Broker::open(config).unwrap();
    broker
        .manage_channel(ChannelManagementCommand {
            operation_id: "kodo-delete-new-0002",
            topic: "events",
            channel: "workers",
            action: ChannelManagementAction::Delete,
            expected_revision: 0,
            tombstone_until_ms: Some(now_ms() + 60_000),
            require_idle: true,
        })
        .await
        .unwrap();
    assert!(!broker.inner.management_ops.lock().blocks_topic("events"));
    broker
        .publish("events", vec![b"accepted".to_vec()], Duration::ZERO)
        .await
        .unwrap();
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
    assert!(reopened.registry_revision() > expected);
}

#[tokio::test]
async fn panicked_storage_tasks_fail_the_broker_closed() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    })
    .unwrap();
    let error = broker
        .storage_task::<()>(|| panic!("injected storage task panic"))
        .await
        .unwrap_err();
    assert!(matches!(error, BrokerError::StorageUnavailable));
    assert!(!broker.storage_healthy());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_storage_task_still_records_its_failure() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    })
    .unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let task = {
        let broker = broker.clone();
        tokio::spawn(async move {
            broker
                .storage_task(move || {
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    Err::<(), _>(BrokerError::InvalidRecord(
                        "injected cancelled storage failure".into(),
                    ))
                })
                .await
        })
    };
    started_rx.recv().unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    release_tx.send(()).unwrap();

    tokio::time::timeout(Duration::from_secs(1), async {
        while broker.storage_healthy() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[test]
fn runtime_integrity_errors_isolate_the_broker() {
    let root = tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        ..BrokerConfig::default()
    })
    .unwrap();

    let error = broker
        .observe_storage_result::<()>(Err(BrokerError::InvalidRecord(
            "corrupt recovery index".into(),
        )))
        .unwrap_err();

    assert!(matches!(error, BrokerError::InvalidRecord(_)));
    assert!(!broker.storage_healthy());
    assert!(matches!(
        broker.ensure_storage_healthy(),
        Err(BrokerError::StorageUnavailable)
    ));
}
