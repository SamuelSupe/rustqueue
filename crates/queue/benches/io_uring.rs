use futures::future::join_all;
use rustqueue_queue::{Broker, BrokerConfig};
use std::time::{Duration, Instant};

const CHANNELS: usize = 16;
const BATCH: usize = 64;
const WARMUP: usize = 20;
const ITERATIONS: usize = 300;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    let io_uring_enabled = std::env::args().any(|arg| arg == "--io-uring");
    if std::env::args().any(|arg| arg == "--read-backlog") {
        let batch_size = std::env::args()
            .find_map(|arg| arg.strip_prefix("--batch-size=").map(str::to_owned))
            .map(|value| value.parse::<usize>())
            .transpose()?
            .unwrap_or(64);
        anyhow::ensure!((1..=64).contains(&batch_size), "batch size must be 1..=64");
        let message_bytes = std::env::args()
            .find_map(|arg| arg.strip_prefix("--message-bytes=").map(str::to_owned))
            .map(|value| value.parse::<usize>())
            .transpose()?
            .unwrap_or(1024);
        anyhow::ensure!(
            (1..=20 * 1024 * 1024).contains(&message_bytes),
            "message size must be 1 byte..=20 MiB"
        );
        return read_backlog(io_uring_enabled, batch_size, message_bytes).await;
    }
    let root = tempfile::tempdir()?;
    let config = BrokerConfig {
        data_path: root.path().into(),
        io_uring_enabled,
        payload_read_workers: 2,
        ..Default::default()
    };
    let broker = Broker::open(config.clone())?;
    let channels: Vec<_> = (0..CHANNELS).map(|i| format!("channel-{i}")).collect();
    for channel in &channels {
        broker.create_channel("fanout", channel).await?;
    }
    let mut timings = Vec::with_capacity(ITERATIONS);
    let mut measured = Duration::ZERO;
    for iteration in 0..WARMUP + ITERATIONS {
        let start = Instant::now();
        let bodies = (0..BATCH).map(|index| vec![index as u8; 128]).collect();
        let ids = broker.publish("fanout", bodies, Duration::ZERO).await?;
        for channel in &channels {
            let messages = broker
                .fetch_batch("fanout", channel, BATCH, BATCH * 1024, Duration::ZERO, None)
                .await?;
            anyhow::ensure!(messages.len() == BATCH, "incomplete fan-out delivery");
            for (index, message) in messages.iter().enumerate() {
                anyhow::ensure!(message.id == ids[index], "delivery ID/order mismatch");
                anyhow::ensure!(message.body.as_ref() == [index as u8; 128], "body mismatch");
            }
        }
        let mut pending = Vec::with_capacity(CHANNELS * BATCH);
        for channel in &channels {
            for id in &ids {
                pending.push(broker.finish("fanout", channel, *id));
            }
        }
        for result in join_all(pending).await {
            result?;
        }
        if iteration >= WARMUP {
            let elapsed = start.elapsed();
            measured += elapsed;
            timings.push(elapsed.as_micros() as u64);
        }
    }
    let groups = broker.stats()?.channel_group_commit;
    timings.sort_unstable();
    drop(broker);
    let mut reopened = config;
    reopened.io_uring_enabled = false;
    let broker = Broker::open(reopened)?;
    for channel in &channels {
        anyhow::ensure!(
            broker
                .next_message("fanout", channel, None)
                .await?
                .is_none(),
            "durably finished messages reappeared after restart"
        );
    }
    println!(
        "{}",
        serde_json::json!({
            "io_uring_enabled": io_uring_enabled,
            "channels": CHANNELS,
            "batch_size": BATCH,
            "message_bytes": 128,
            "iterations": ITERATIONS,
            "deliveries": CHANNELS * BATCH * ITERATIONS,
            "delivery_messages_per_second": (CHANNELS * BATCH * ITERATIONS) as f64 / measured.as_secs_f64(),
            "elapsed_seconds": measured.as_secs_f64(),
            "round_p99_us": timings[ITERATIONS * 99 / 100 - 1],
            "channel_group_commits": groups.commits,
            "max_group_requests": groups.max_batch_requests,
            "recovery_verified": true,
        })
    );
    Ok(())
}

async fn read_backlog(
    io_uring_enabled: bool,
    batch_size: usize,
    message_bytes: usize,
) -> anyhow::Result<()> {
    let message_count = (32 * 1024 * 1024 / message_bytes).clamp(64, 32768);
    let warmup_count = (2 * 1024 * 1024 / message_bytes).clamp(2, 2048);
    let root = tempfile::tempdir()?;
    let broker = Broker::open(BrokerConfig {
        data_path: root.path().into(),
        io_uring_enabled,
        entry_cache_bytes: 1,
        payload_read_workers: 2,
        ..Default::default()
    })?;
    broker.create_channel("backlog", "reader").await?;
    let total = message_count + warmup_count;
    let mut ids = Vec::with_capacity(total);
    let publish_batch = (1024 * 1024 / message_bytes).clamp(1, 1024);
    for offset in (0..total).step_by(publish_batch) {
        ids.extend(
            broker
                .publish(
                    "backlog",
                    (offset..(offset + publish_batch).min(total))
                        .map(|index| vec![index as u8; message_bytes])
                        .collect(),
                    Duration::ZERO,
                )
                .await?,
        );
    }
    let mut position = 0;
    let mut elapsed = Duration::ZERO;
    let mut timings = Vec::new();
    while position < ids.len() {
        let limit = if position < warmup_count {
            warmup_count
        } else {
            ids.len()
        };
        let count = batch_size.min(limit - position);
        let start = Instant::now();
        let messages = broker
            .fetch_batch(
                "backlog",
                "reader",
                count,
                count * message_bytes,
                Duration::ZERO,
                None,
            )
            .await?;
        let duration = start.elapsed();
        anyhow::ensure!(
            !messages.is_empty() && messages.len() <= count,
            "backlog stopped before all messages were delivered"
        );
        for (index, message) in messages.iter().enumerate() {
            let expected = position + index;
            anyhow::ensure!(message.id == ids[expected], "backlog ID/order mismatch");
            anyhow::ensure!(
                message.body.len() == message_bytes
                    && message.body.iter().all(|byte| *byte == expected as u8),
                "backlog body mismatch"
            );
        }
        if position >= warmup_count {
            elapsed += duration;
            timings.push(duration.as_micros() as u64);
        }
        position += messages.len();
    }
    timings.sort_unstable();
    println!(
        "{}",
        serde_json::json!({
            "mode": "read_backlog",
            "io_uring_enabled": io_uring_enabled,
            "batch_size": batch_size,
            "message_bytes": message_bytes,
            "messages": message_count,
            "read_messages_per_second": message_count as f64 / elapsed.as_secs_f64(),
            "fetch_p99_us": timings[(timings.len() * 99 / 100).saturating_sub(1)],
            "delivery_verified": true,
        })
    );
    Ok(())
}
