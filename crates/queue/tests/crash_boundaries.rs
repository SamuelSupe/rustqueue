#![cfg(all(unix, feature = "crash-injection"))]

use rustqueue_queue::{Broker, BrokerConfig};
use std::collections::BTreeSet;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

fn config(path: &Path) -> BrokerConfig {
    BrokerConfig {
        data_path: path.into(),
        node_id: 41,
        max_segment_bytes: 256,
        max_message_bytes: 1024,
        bootstrap_retention: Duration::ZERO,
        ..BrokerConfig::default()
    }
}

fn spawn_until_marker(path: &Path, scenario: &str, failpoint: &str) {
    let marker = path.join(format!("{failpoint}.ready"));
    let mut child = Command::new(env!("CARGO_BIN_EXE_rustqueue-crash-worker"))
        .arg(scenario)
        .arg(path)
        .env("RUSTQUEUE_CRASH_FAILPOINT", failpoint)
        .env("RUSTQUEUE_CRASH_MARKER", &marker)
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !marker.exists() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("crash worker exited before {failpoint}: {status}");
        }
        assert!(
            Instant::now() < deadline,
            "crash worker did not reach {failpoint}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let result = unsafe { libc::kill(child.id() as i32, libc::SIGKILL) };
    assert_eq!(result, 0);
    let status = child.wait().unwrap();
    assert!(!status.success());
}

async fn publish_fixture(path: &Path) {
    let broker = Broker::open(config(path)).unwrap();
    broker.create_channel("events", "workers").await.unwrap();
    broker
        .publish("events", vec![b"acknowledged".to_vec()], Duration::ZERO)
        .await
        .unwrap();
}

async fn bodies(path: &Path) -> BTreeSet<Vec<u8>> {
    let broker = Broker::open(config(path)).unwrap();
    broker
        .fetch_batch("events", "workers", 16, 16 * 1024, Duration::ZERO, None)
        .await
        .unwrap()
        .into_iter()
        .map(|message| message.body.to_vec())
        .collect()
}

#[tokio::test]
async fn sigkill_publish_boundaries_preserve_the_acknowledged_ledger() {
    for failpoint in [
        "publish_after_append_before_fsync",
        "publish_after_fsync_before_reply",
    ] {
        let root = tempfile::tempdir().unwrap();
        publish_fixture(root.path()).await;
        spawn_until_marker(root.path(), "publish", failpoint);
        let recovered = bodies(root.path()).await;
        assert!(recovered.contains(b"acknowledged".as_slice()));
        if failpoint == "publish_after_fsync_before_reply" {
            assert!(recovered.contains(b"ambiguous".as_slice()));
        }
    }
}

#[tokio::test]
async fn sigkill_channel_wal_boundaries_are_at_least_once() {
    for (failpoint, must_be_finished) in [
        ("channel_after_wal_append_before_fsync", false),
        ("channel_after_wal_fsync_before_return", true),
    ] {
        let root = tempfile::tempdir().unwrap();
        publish_fixture(root.path()).await;
        spawn_until_marker(root.path(), "finish", failpoint);
        let recovered = bodies(root.path()).await;
        if must_be_finished {
            assert!(!recovered.contains(b"acknowledged".as_slice()));
        } else {
            assert!(recovered.len() <= 1);
        }
    }
}

#[tokio::test]
async fn sigkill_checkpoint_replays_the_old_checkpoint_and_wal() {
    let root = tempfile::tempdir().unwrap();
    publish_fixture(root.path()).await;
    {
        let broker = Broker::open(config(root.path())).unwrap();
        let message = broker
            .next_message("events", "workers", None)
            .await
            .unwrap()
            .unwrap();
        broker
            .finish("events", "workers", message.id)
            .await
            .unwrap();
    }
    spawn_until_marker(
        root.path(),
        "checkpoint",
        "checkpoint_after_file_fsync_before_rename",
    );
    assert!(bodies(root.path()).await.is_empty());
}

#[tokio::test]
async fn sigkill_gc_boundaries_reopen_without_corrupting_the_topic() {
    for failpoint in [
        "gc_before_segment_delete",
        "gc_after_segment_delete_before_dir_fsync",
    ] {
        let root = tempfile::tempdir().unwrap();
        let broker = Broker::open(config(root.path())).unwrap();
        broker.create_channel("events", "workers").await.unwrap();
        for value in 0..4u8 {
            broker
                .publish("events", vec![vec![value; 120]], Duration::ZERO)
                .await
                .unwrap();
        }
        for message in broker
            .fetch_batch("events", "workers", 8, 4096, Duration::ZERO, None)
            .await
            .unwrap()
        {
            broker
                .finish("events", "workers", message.id)
                .await
                .unwrap();
        }
        let survivor = broker
            .publish("events", vec![b"survivor".to_vec()], Duration::ZERO)
            .await
            .unwrap()[0];
        drop(broker);
        spawn_until_marker(root.path(), "gc", failpoint);

        let broker = Broker::open(config(root.path())).unwrap();
        let recovered = broker
            .next_message("events", "workers", None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(recovered.id, survivor);
        assert_eq!(recovered.body.as_ref(), b"survivor");
        broker
            .finish("events", "workers", recovered.id)
            .await
            .unwrap();
        let id = broker
            .publish("events", vec![b"fresh".to_vec()], Duration::ZERO)
            .await
            .unwrap()[0];
        assert_eq!(
            broker
                .next_message("events", "workers", None)
                .await
                .unwrap()
                .unwrap()
                .id,
            id
        );
    }
}
