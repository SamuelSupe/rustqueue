use super::Args;
use anyhow::Context;
use hdrhistogram::Histogram;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

#[derive(Clone, Copy)]
struct MessageShape {
    bytes: usize,
    batch_size: usize,
}

pub(crate) async fn run_workers(
    args: &Args,
    topic: &str,
    total_messages: Option<u64>,
    duration: Option<Duration>,
    histogram: Arc<Mutex<Histogram<u64>>>,
) -> anyhow::Result<u64> {
    let base = total_messages.map(|messages| messages / args.producers as u64);
    let remainder = total_messages.map(|messages| messages % args.producers as u64);
    let deadline = duration.map(|duration| Instant::now() + duration);
    let mut tasks = tokio::task::JoinSet::new();
    for producer in 0..args.producers {
        let count = base.map(|base| {
            base + u64::from((producer as u64) < remainder.expect("count has remainder"))
        });
        let address = args.address.clone();
        let topic = topic.to_owned();
        let histogram = Arc::clone(&histogram);
        let producer_rate = args.rate.map(|rate| {
            let base = rate / args.producers as u64;
            base + u64::from((producer as u64) < rate % args.producers as u64)
        });
        let shape = MessageShape {
            bytes: args.message_bytes,
            batch_size: args.batch_size,
        };
        tasks.spawn(async move {
            publish_worker(
                &address,
                &topic,
                count,
                deadline,
                shape,
                producer_rate,
                histogram,
            )
            .await
        });
    }
    let mut messages = 0u64;
    while let Some(result) = tasks.join_next().await {
        messages = messages.saturating_add(result.context("benchmark worker panicked")??);
    }
    Ok(messages)
}

async fn publish_worker(
    address: &str,
    topic: &str,
    count: Option<u64>,
    deadline: Option<Instant>,
    shape: MessageShape,
    rate: Option<u64>,
    histogram: Arc<Mutex<Histogram<u64>>>,
) -> anyhow::Result<u64> {
    let MessageShape {
        bytes: message_bytes,
        batch_size,
    } = shape;
    let mut stream = TcpStream::connect(address)
        .await
        .with_context(|| format!("connect to {address}"))?;
    stream.set_nodelay(true)?;
    stream.write_all(b"  V2").await?;
    let command = format!("{} {topic}\n", if batch_size == 1 { "PUB" } else { "MPUB" });
    let body = vec![b'x'; message_bytes];
    let max_batch_body = 4usize
        .checked_add(
            batch_size
                .checked_mul(4usize.saturating_add(message_bytes))
                .context("batch body length overflow")?,
        )
        .context("batch body length overflow")?;
    if max_batch_body > u32::MAX as usize {
        anyhow::bail!("batch body exceeds the NSQ 32-bit frame limit");
    }
    let mut local = Histogram::<u64>::new_with_max(60_000_000, 3)?;
    let period = match rate {
        Some(0) => anyhow::bail!("fixed rate is lower than producer count"),
        Some(rate) => {
            let period_ns = 1_000_000_000u64 / rate.max(1);
            Some(Duration::from_nanos(period_ns.max(1)))
        }
        None => None,
    };
    let mut scheduled = Instant::now();

    let mut sent = 0u64;
    loop {
        if count.is_some_and(|count| sent >= count)
            || deadline.is_some_and(|deadline| Instant::now() >= deadline)
        {
            break;
        }
        let send_messages = count
            .map(|count| count.saturating_sub(sent).min(batch_size as u64))
            .unwrap_or(batch_size as u64) as usize;
        let started = if let Some(period) = period {
            scheduled += period
                .checked_mul(u32::try_from(send_messages).context("batch-size is too large")?)
                .context("fixed-rate schedule overflow")?;
            tokio::time::sleep_until(tokio::time::Instant::from_std(scheduled)).await;
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                break;
            }
            scheduled
        } else {
            Instant::now()
        };
        stream.write_all(command.as_bytes()).await?;
        if batch_size == 1 {
            stream
                .write_all(&(message_bytes as u32).to_be_bytes())
                .await?;
            stream.write_all(&body).await?;
        } else {
            let batch_body = 4usize + send_messages * (4 + message_bytes);
            stream.write_all(&(batch_body as u32).to_be_bytes()).await?;
            stream
                .write_all(&(send_messages as u32).to_be_bytes())
                .await?;
            for _ in 0..send_messages {
                stream
                    .write_all(&(message_bytes as u32).to_be_bytes())
                    .await?;
                stream.write_all(&body).await?;
            }
        }
        loop {
            let size = stream.read_u32().await? as usize;
            if !(4..=1024).contains(&size) {
                anyhow::bail!("server returned invalid frame size {size}");
            }
            let frame_type = stream.read_i32().await?;
            let mut response = vec![0; size - 4];
            stream.read_exact(&mut response).await?;
            if frame_type == 0 && response == b"_heartbeat_" {
                stream.write_all(b"NOP\n").await?;
                continue;
            }
            if frame_type != 0 || response != b"OK" {
                anyhow::bail!("publish failed: {}", String::from_utf8_lossy(&response));
            }
            break;
        }
        let latency = started.elapsed().as_micros().min(u64::MAX as u128) as u64;
        local.record(latency.max(1))?;
        sent += send_messages as u64;
    }
    histogram.lock().await.add(&local)?;
    Ok(sent)
}
