use anyhow::Context;
use clap::Parser;
use hdrhistogram::Histogram;
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch, Mutex};

#[derive(Parser, Debug)]
#[command(name = "rustqueue-bench", about = "NSQ V2 durable publish benchmark")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:4150")]
    address: String,
    #[arg(long, default_value = "benchmark")]
    topic: String,
    #[arg(long, default_value_t = 100_000)]
    messages: u64,
    #[arg(long, default_value_t = 1024)]
    message_bytes: usize,
    #[arg(long, default_value_t = 16)]
    producers: usize,
    #[arg(long, default_value_t = 16)]
    consumers: usize,
    #[arg(
        long,
        default_value_t = 1,
        help = "Messages per PUB/MPUB acknowledgement"
    )]
    batch_size: usize,
    #[arg(long, default_value_t = 0)]
    warmup_seconds: u64,
    #[arg(
        long,
        help = "Measure for this duration instead of stopping at --messages"
    )]
    duration_seconds: Option<u64>,
    #[arg(long, help = "Total fixed arrival rate; omit for saturation mode")]
    rate: Option<u64>,
    #[arg(long, default_value_t = false)]
    json: bool,
}

#[derive(Serialize)]
struct Report {
    address: String,
    topic: String,
    messages: u64,
    message_bytes: usize,
    producers: usize,
    consumers: usize,
    batch_size: usize,
    mode: &'static str,
    elapsed_seconds: f64,
    messages_per_second: f64,
    mebibytes_per_second: f64,
    latency_us_p50: u64,
    latency_us_p95: u64,
    latency_us_p99: u64,
    latency_us_p999: u64,
    latency_us_max: u64,
}

#[derive(Clone, Copy)]
struct MessageShape {
    bytes: usize,
    batch_size: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.producers == 0 || args.messages == 0 || args.message_bytes == 0 || args.batch_size == 0
    {
        anyhow::bail!(
            "producers, messages, message-bytes and batch-size must be greater than zero"
        );
    }
    if args.duration_seconds == Some(0) {
        anyhow::bail!("duration-seconds must be greater than zero when specified");
    }
    if args.topic.len() > 64 {
        anyhow::bail!("topic exceeds NSQ's 64-byte limit");
    }
    let (stop_consumers, consumer_tasks) = if args.consumers == 0 {
        let (stop, _) = watch::channel(false);
        (stop, Vec::new())
    } else {
        start_consumers(&args).await?
    };
    if args.warmup_seconds > 0 {
        let warmup = Arc::new(Mutex::new(Histogram::<u64>::new_with_max(60_000_000, 3)?));
        run_workers(
            &args,
            None,
            Some(Duration::from_secs(args.warmup_seconds)),
            warmup,
        )
        .await?;
    }
    let histogram = Arc::new(Mutex::new(Histogram::<u64>::new_with_max(60_000_000, 3)?));
    let start = Instant::now();
    let measured_messages = run_workers(
        &args,
        args.duration_seconds.is_none().then_some(args.messages),
        args.duration_seconds.map(Duration::from_secs),
        Arc::clone(&histogram),
    )
    .await?;
    let elapsed = start.elapsed();
    let _ = stop_consumers.send(true);
    for task in consumer_tasks {
        task.await.context("benchmark consumer panicked")??;
    }
    let histogram = histogram.lock().await;
    let seconds = elapsed.as_secs_f64();
    let report = Report {
        address: args.address,
        topic: args.topic,
        messages: measured_messages,
        message_bytes: args.message_bytes,
        producers: args.producers,
        consumers: args.consumers,
        batch_size: args.batch_size,
        mode: if args.rate.is_some() {
            "fixed-rate"
        } else {
            "saturation"
        },
        elapsed_seconds: seconds,
        messages_per_second: measured_messages as f64 / seconds,
        mebibytes_per_second: measured_messages as f64 * args.message_bytes as f64
            / seconds
            / (1024.0 * 1024.0),
        latency_us_p50: histogram.value_at_quantile(0.50),
        latency_us_p95: histogram.value_at_quantile(0.95),
        latency_us_p99: histogram.value_at_quantile(0.99),
        latency_us_p999: histogram.value_at_quantile(0.999),
        latency_us_max: histogram.max(),
    };
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!(
            "{} messages in {:.3}s: {:.0} msg/s, {:.2} MiB/s",
            report.messages,
            report.elapsed_seconds,
            report.messages_per_second,
            report.mebibytes_per_second
        );
        println!(
            "publish ACK latency (us): p50={} p95={} p99={} p99.9={} max={}",
            report.latency_us_p50,
            report.latency_us_p95,
            report.latency_us_p99,
            report.latency_us_p999,
            report.latency_us_max
        );
    }
    Ok(())
}

async fn start_consumers(
    args: &Args,
) -> anyhow::Result<(
    watch::Sender<bool>,
    Vec<tokio::task::JoinHandle<anyhow::Result<()>>>,
)> {
    let (stop, stop_rx) = watch::channel(false);
    let (ready, mut ready_rx) = mpsc::channel(args.consumers);
    let mut tasks = Vec::with_capacity(args.consumers);
    for _ in 0..args.consumers {
        tasks.push(tokio::spawn(consume_worker(
            args.address.clone(),
            args.topic.clone(),
            ready.clone(),
            stop_rx.clone(),
        )));
    }
    drop(ready);
    for _ in 0..args.consumers {
        ready_rx
            .recv()
            .await
            .context("consumer exited before subscription became ready")?;
    }
    Ok((stop, tasks))
}

async fn consume_worker(
    address: String,
    topic: String,
    ready: mpsc::Sender<()>,
    mut stop: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut stream = TcpStream::connect(&address)
        .await
        .with_context(|| format!("connect consumer to {address}"))?;
    stream.set_nodelay(true)?;
    stream.write_all(b"  V2").await?;
    stream
        .write_all(format!("SUB {topic} benchmark\n").as_bytes())
        .await?;
    loop {
        let (frame_type, response) = read_frame(&mut stream).await?;
        if frame_type == 0 && response == b"_heartbeat_" {
            stream.write_all(b"NOP\n").await?;
        } else if frame_type == 0 && response == b"OK" {
            break;
        } else if frame_type == 1 {
            anyhow::bail!("subscribe failed: {}", String::from_utf8_lossy(&response));
        }
    }
    stream.write_all(b"RDY 2500\n").await?;
    ready.send(()).await.ok();

    loop {
        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    return Ok(());
                }
            }
            frame = read_frame(&mut stream) => {
                let (frame_type, response) = match frame {
                    Ok(frame) => frame,
                    Err(_) if *stop.borrow() => return Ok(()),
                    Err(error) => return Err(error),
                };
                match frame_type {
                    0 if response == b"_heartbeat_" => stream.write_all(b"NOP\n").await?,
                    2 if response.len() >= 26 => {
                        let id = &response[10..26];
                        stream.write_all(b"FIN ").await?;
                        stream.write_all(id).await?;
                        stream.write_all(b"\n").await?;
                    }
                    1 => anyhow::bail!(
                        "consumer error: {}",
                        String::from_utf8_lossy(&response)
                    ),
                    _ => {}
                }
            }
        }
    }
}

async fn read_frame(stream: &mut TcpStream) -> anyhow::Result<(i32, Vec<u8>)> {
    let size = stream.read_u32().await? as usize;
    if !(4..=16 * 1024 * 1024).contains(&size) {
        anyhow::bail!("server returned invalid frame size {size}");
    }
    let frame_type = stream.read_i32().await?;
    let mut response = vec![0; size - 4];
    stream.read_exact(&mut response).await?;
    Ok((frame_type, response))
}

async fn run_workers(
    args: &Args,
    total_messages: Option<u64>,
    duration: Option<Duration>,
    histogram: Arc<Mutex<Histogram<u64>>>,
) -> anyhow::Result<u64> {
    let base = total_messages.map(|messages| messages / args.producers as u64);
    let remainder = total_messages.map(|messages| messages % args.producers as u64);
    let deadline = duration.map(|duration| Instant::now() + duration);
    let mut tasks = Vec::with_capacity(args.producers);
    for producer in 0..args.producers {
        let count = base.map(|base| {
            base + u64::from((producer as u64) < remainder.expect("count has remainder"))
        });
        let address = args.address.clone();
        let topic = args.topic.clone();
        let message_bytes = args.message_bytes;
        let batch_size = args.batch_size;
        let histogram = Arc::clone(&histogram);
        let producer_rate = args.rate.map(|rate| {
            let base = rate / args.producers as u64;
            base + u64::from((producer as u64) < rate % args.producers as u64)
        });
        tasks.push(tokio::spawn(async move {
            publish_worker(
                &address,
                &topic,
                count,
                deadline,
                MessageShape {
                    bytes: message_bytes,
                    batch_size,
                },
                producer_rate,
                histogram,
            )
            .await
        }));
    }
    let mut messages = 0u64;
    for task in tasks {
        messages = messages.saturating_add(task.await.context("benchmark worker panicked")??);
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
