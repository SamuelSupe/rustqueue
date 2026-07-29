mod consumer;
mod producer;

use clap::Parser;
use consumer::{start_consumers, ConsumerProgress, DeliverySnapshot};
use hdrhistogram::Histogram;
use producer::run_workers;
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

#[derive(Parser, Debug)]
#[command(
    name = "rustqueue-bench",
    about = "NSQ V2 publish and delivery benchmark"
)]
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
    #[arg(
        long,
        default_value_t = false,
        help = "Use --topic verbatim; existing retained messages can affect receive counts"
    )]
    reuse_topic: bool,
    #[arg(
        long,
        default_value_t = 300,
        help = "Maximum seconds to wait for consumers after publishing"
    )]
    drain_timeout_seconds: u64,
}

#[derive(Serialize)]
struct Report {
    address: String,
    requested_topic: String,
    topic: String,
    channel: Option<String>,
    messages: u64,
    received_unique_messages: u64,
    received_total_messages: u64,
    duplicate_messages: u64,
    missing_messages: u64,
    delivery_verified: bool,
    delivery_complete: bool,
    drain_timed_out: bool,
    message_bytes: usize,
    producers: usize,
    consumers: usize,
    batch_size: usize,
    mode: &'static str,
    elapsed_seconds: f64,
    messages_per_second: f64,
    mebibytes_per_second: f64,
    publish_elapsed_seconds: f64,
    publish_messages_per_second: f64,
    drain_elapsed_seconds: f64,
    end_to_end_elapsed_seconds: Option<f64>,
    receive_messages_per_second: Option<f64>,
    receive_mebibytes_per_second: Option<f64>,
    latency_us_p50: u64,
    latency_us_p95: u64,
    latency_us_p99: u64,
    latency_us_p999: u64,
    latency_us_max: u64,
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
    if args.drain_timeout_seconds == 0 {
        anyhow::bail!("drain-timeout-seconds must be greater than zero");
    }
    if args.topic.len() > 64 {
        anyhow::bail!("topic exceeds NSQ's 64-byte limit");
    }
    if !args.topic.is_ascii() {
        anyhow::bail!("topic must use NSQ's ASCII name character set");
    }
    if args.warmup_seconds > 0 {
        let warmup_topic = isolated_topic(&args.topic, "warmup");
        let warmup_progress = (args.consumers > 0).then(|| Arc::new(ConsumerProgress::default()));
        let mut warmup_group = match &warmup_progress {
            None => None,
            Some(progress) => {
                let channel = benchmark_name("warmup-channel");
                Some(
                    start_consumers(
                        &args.address,
                        &warmup_topic,
                        &channel,
                        args.consumers,
                        Arc::clone(progress),
                    )
                    .await?,
                )
            }
        };
        let warmup = Arc::new(Mutex::new(Histogram::<u64>::new_with_max(60_000_000, 3)?));
        let warmup_result = if let Some(group) = warmup_group.as_mut() {
            tokio::select! {
                result = run_workers(
                    &args,
                    &warmup_topic,
                    None,
                    Some(Duration::from_secs(args.warmup_seconds)),
                    warmup,
                ) => result,
                failure = group.failure() => {
                    Err(anyhow::anyhow!("warmup consumer failed: {failure}"))
                }
            }
        } else {
            run_workers(
                &args,
                &warmup_topic,
                None,
                Some(Duration::from_secs(args.warmup_seconds)),
                warmup,
            )
            .await
        };
        if let Some(group) = warmup_group.take() {
            let published = match warmup_result {
                Ok(published) => published,
                Err(error) => {
                    let _ = group.stop().await;
                    return Err(error);
                }
            };
            let progress = warmup_progress
                .as_ref()
                .expect("warmup consumers have delivery progress");
            let delivery = progress
                .wait_for(published, Duration::from_secs(args.drain_timeout_seconds))
                .await;
            let stop_result = group.stop().await;
            require_complete_delivery(
                true,
                delivery.complete,
                delivery.snapshot.unique,
                delivery.snapshot.duplicates(),
                published,
            )?;
            stop_result?;
        } else {
            warmup_result?;
        }
    }

    let delivery_progress = (args.consumers > 0).then(|| Arc::new(ConsumerProgress::default()));
    let measured_topic = if args.reuse_topic {
        args.topic.clone()
    } else {
        isolated_topic(&args.topic, "run")
    };
    let channel = delivery_progress
        .as_ref()
        .map(|_| benchmark_name("measured"));
    let mut consumer_group = match (&delivery_progress, &channel) {
        (Some(progress), Some(channel)) => Some(
            start_consumers(
                &args.address,
                &measured_topic,
                channel,
                args.consumers,
                Arc::clone(progress),
            )
            .await?,
        ),
        _ => None,
    };
    let histogram = Arc::new(Mutex::new(Histogram::<u64>::new_with_max(60_000_000, 3)?));
    let start = Instant::now();
    let measured_result = if let Some(group) = consumer_group.as_mut() {
        tokio::select! {
            result = run_workers(
                &args,
                &measured_topic,
                args.duration_seconds.is_none().then_some(args.messages),
                args.duration_seconds.map(Duration::from_secs),
                Arc::clone(&histogram),
            ) => result,
            failure = group.failure() => {
                Err(anyhow::anyhow!("benchmark consumer failed: {failure}"))
            }
        }
    } else {
        run_workers(
            &args,
            &measured_topic,
            args.duration_seconds.is_none().then_some(args.messages),
            args.duration_seconds.map(Duration::from_secs),
            Arc::clone(&histogram),
        )
        .await
    };
    let publish_elapsed = start.elapsed();
    let measured_messages = match measured_result {
        Ok(messages) => messages,
        Err(error) => {
            if let Some(group) = consumer_group.take() {
                let _ = group.stop().await;
            }
            return Err(error);
        }
    };
    if measured_messages == 0 {
        if let Some(group) = consumer_group.take() {
            let _ = group.stop().await;
        }
        anyhow::bail!("benchmark published no messages during the measurement window");
    }

    let drain_start = Instant::now();
    let delivery_result = match (&delivery_progress, consumer_group.as_mut()) {
        (Some(progress), Some(group)) => {
            tokio::select! {
                waited = progress.wait_for(
                    measured_messages,
                    Duration::from_secs(args.drain_timeout_seconds),
                ) => Ok((waited.snapshot, waited.complete)),
                failure = group.failure() => {
                    Err(anyhow::anyhow!("benchmark consumer failed: {failure}"))
                }
            }
        }
        _ => Ok((DeliverySnapshot::default(), false)),
    };
    let (delivery, delivery_complete) = match delivery_result {
        Ok(delivery) => delivery,
        Err(error) => {
            if let Some(group) = consumer_group.take() {
                let _ = group.stop().await;
            }
            return Err(error);
        }
    };
    let receive_elapsed = delivery_progress.as_ref().map(|_| start.elapsed());
    let drain_elapsed = delivery_progress
        .as_ref()
        .map_or(Duration::ZERO, |_| drain_start.elapsed());
    if let Some(group) = consumer_group.take() {
        group.stop().await?;
    }

    let histogram = histogram.lock().await;
    let publish_seconds = publish_elapsed.as_secs_f64();
    let received_unique = delivery.unique;
    let missing_messages = measured_messages.saturating_sub(received_unique);
    let receive_seconds = receive_elapsed.map(|elapsed| elapsed.as_secs_f64());
    let report = Report {
        address: args.address,
        requested_topic: args.topic,
        topic: measured_topic,
        channel,
        messages: measured_messages,
        received_unique_messages: received_unique,
        received_total_messages: delivery.total,
        duplicate_messages: delivery.duplicates(),
        missing_messages,
        delivery_verified: delivery_progress.is_some(),
        delivery_complete,
        drain_timed_out: delivery_progress.is_some() && !delivery_complete,
        message_bytes: args.message_bytes,
        producers: args.producers,
        consumers: args.consumers,
        batch_size: args.batch_size,
        mode: if args.rate.is_some() {
            "fixed-rate"
        } else {
            "saturation"
        },
        elapsed_seconds: publish_seconds,
        messages_per_second: measured_messages as f64 / publish_seconds,
        mebibytes_per_second: measured_messages as f64 * args.message_bytes as f64
            / publish_seconds
            / (1024.0 * 1024.0),
        publish_elapsed_seconds: publish_seconds,
        publish_messages_per_second: measured_messages as f64 / publish_seconds,
        drain_elapsed_seconds: drain_elapsed.as_secs_f64(),
        end_to_end_elapsed_seconds: receive_seconds,
        receive_messages_per_second: receive_seconds
            .map(|seconds| received_unique as f64 / seconds),
        receive_mebibytes_per_second: receive_seconds.map(|seconds| {
            received_unique as f64 * args.message_bytes as f64 / seconds / (1024.0 * 1024.0)
        }),
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
            "publish ACK: {} messages in {:.3}s, {:.0} msg/s, {:.2} MiB/s",
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
        if report.delivery_verified {
            println!(
                "receive: {} unique / {} published in {:.3}s, {:.0} msg/s; duplicates={}, missing={}, drain={:.3}s{}",
                report.received_unique_messages,
                report.messages,
                report.end_to_end_elapsed_seconds.unwrap_or_default(),
                report.receive_messages_per_second.unwrap_or_default(),
                report.duplicate_messages,
                report.missing_messages,
                report.drain_elapsed_seconds,
                if report.delivery_complete {
                    ""
                } else {
                    " (drain timeout)"
                }
            );
        } else {
            println!("receive: not measured (--consumers=0)");
        }
    }
    require_complete_delivery(
        report.delivery_verified,
        report.delivery_complete,
        report.received_unique_messages,
        report.duplicate_messages,
        report.messages,
    )?;
    Ok(())
}

fn require_complete_delivery(
    verified: bool,
    complete: bool,
    received: u64,
    duplicates: u64,
    published: u64,
) -> anyhow::Result<()> {
    if verified && duplicates > 0 {
        anyhow::bail!(
            "delivery verification failed: observed {duplicates} unexpected duplicate deliveries"
        );
    }
    if verified && !complete {
        anyhow::bail!(
            "delivery verification failed: received {received} unique messages out of {published} before the drain timeout"
        );
    }
    Ok(())
}

fn benchmark_name(label: &str) -> String {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("bench-{label}-{}-{nonce}", std::process::id())
}

fn isolated_topic(base: &str, label: &str) -> String {
    let suffix = format!("-{label}-{}", benchmark_name("topic"));
    let keep = 64usize.saturating_sub(suffix.len());
    format!("{}{}", &base[..base.len().min(keep)], suffix)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn benchmark_channels_are_durable_isolated_and_nsq_compatible() {
        let channel = benchmark_name("measured");
        assert!(!channel.ends_with("#ephemeral"));
        assert!(channel.len() <= 64);
    }

    #[test]
    fn isolated_topics_are_nsq_bounded() {
        let topic = isolated_topic(&"x".repeat(64), "run");
        assert_eq!(topic.len(), 64);
        assert!(topic.contains("-run-bench-topic-"));
    }

    #[test]
    fn incomplete_verified_delivery_fails_the_benchmark() {
        assert!(require_complete_delivery(true, false, 99, 0, 100).is_err());
        assert!(require_complete_delivery(true, true, 100, 0, 100).is_ok());
        assert!(require_complete_delivery(false, false, 0, 0, 100).is_ok());
    }

    #[test]
    fn unexpected_duplicate_delivery_fails_the_benchmark() {
        assert!(require_complete_delivery(true, true, 100, 1, 100).is_err());
    }
}
