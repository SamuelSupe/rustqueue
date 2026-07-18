use crate::admission::PublishAdmission;
use crate::config::Config;
use crate::metrics::Metrics;
use anyhow::Context;
use rustqueue_queue::Broker;
use rustqueue_storage::{disk_space, DiskSpace};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

const POLL_INTERVAL: Duration = Duration::from_secs(1);
const GC_INTERVAL: Duration = Duration::from_secs(5);
const GC_TOPICS_PER_TICK: usize = 128;

pub fn initialize(
    config: &Config,
    admission: &PublishAdmission,
    metrics: &Metrics,
) -> anyhow::Result<bool> {
    let space = disk_space(&config.storage.data_path).context("inspect broker data disk")?;
    record_space(metrics, space);
    let pressured = high(config, space);
    admission.set_storage_ready(!pressured);
    metrics
        .disk_pressure
        .store(i64::from(pressured), Ordering::Release);
    Ok(pressured)
}

pub async fn run(
    config: Arc<Config>,
    broker: Arc<Broker>,
    admission: Arc<PublishAdmission>,
    metrics: Arc<Metrics>,
    initially_pressured: bool,
) -> anyhow::Result<()> {
    let mut pressure_since = initially_pressured.then(Instant::now);
    let started = Instant::now();
    let gc_not_before = if initially_pressured {
        started
    } else {
        started + Duration::from_secs(config.storage.maintenance_startup_delay_seconds)
    };
    let mut last_gc = started;
    let mut tick = tokio::time::interval(POLL_INTERVAL);
    loop {
        tick.tick().await;
        if Instant::now() >= gc_not_before && last_gc.elapsed() >= GC_INTERVAL {
            let removed = broker
                .compact_some(GC_TOPICS_PER_TICK)
                .await
                .context("compact local queue segments")?;
            if removed > 0 {
                info!(segments = removed, "garbage-collected queue segments");
            }
            last_gc = Instant::now();
        }
        let space = match disk_space(&config.storage.data_path) {
            Ok(space) => space,
            Err(error) => {
                admission.set_storage_ready(false);
                metrics.disk_pressure.store(1, Ordering::Release);
                pressure_since.get_or_insert_with(Instant::now);
                warn!(%error, "disk inspection failed; publishing is fail-closed");
                continue;
            }
        };
        record_space(&metrics, space);
        if pressure_since.is_none() && high(&config, space) {
            admission.set_storage_ready(false);
            metrics.disk_pressure.store(1, Ordering::Release);
            pressure_since = Some(Instant::now());
            warn!(
                available_bytes = space.available_bytes,
                used_percent = space.used_percent,
                "disk high watermark reached; publishing is throttled"
            );
        }
        let Some(since) = pressure_since else {
            continue;
        };
        if recovered(&config, space) {
            admission.set_storage_ready(true);
            metrics.disk_pressure.store(0, Ordering::Release);
            pressure_since = None;
            info!(
                available_bytes = space.available_bytes,
                used_percent = space.used_percent,
                "disk pressure cleared; publishing resumed"
            );
            continue;
        }
        admission.set_storage_ready(false);
        if !config.storage.protective_eviction_enabled
            || since.elapsed() < Duration::from_secs(config.storage.disk_pressure_grace_seconds)
        {
            continue;
        }
        if let Some(report) = broker
            .protective_evict_oldest()
            .await
            .context("perform protective local eviction")?
        {
            metrics.protective_evictions.fetch_add(1, Ordering::Relaxed);
            metrics
                .protective_evicted_messages
                .fetch_add(report.messages, Ordering::Relaxed);
            warn!(
                topic = report.topic,
                through_position = report.through_position,
                messages = report.messages,
                "protectively evicted oldest local segment"
            );
        }
    }
}

fn high(config: &Config, space: DiskSpace) -> bool {
    space.used_percent >= config.storage.disk_high_watermark_percent
        || space.available_bytes < config.storage.min_free_bytes
}

fn recovered(config: &Config, space: DiskSpace) -> bool {
    space.used_percent < config.storage.disk_low_watermark_percent
        && space.available_bytes >= config.storage.min_free_bytes
}

fn record_space(metrics: &Metrics, space: DiskSpace) {
    metrics
        .disk_total_bytes
        .store(space.total_bytes, Ordering::Relaxed);
    metrics
        .disk_available_bytes
        .store(space.available_bytes, Ordering::Relaxed);
    metrics
        .disk_used_percent
        .store(space.used_percent as u64, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn applies_high_and_low_hysteresis() {
        let mut config = Config::default();
        config.storage.min_free_bytes = 100;
        let high_space = DiskSpace {
            total_bytes: 1000,
            available_bytes: 100,
            used_percent: 90,
        };
        let low_space = DiskSpace {
            total_bytes: 1000,
            available_bytes: 300,
            used_percent: 70,
        };
        assert!(high(&config, high_space));
        assert!(!recovered(&config, high_space));
        assert!(recovered(&config, low_space));
    }
}
