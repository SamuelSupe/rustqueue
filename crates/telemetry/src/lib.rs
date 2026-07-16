use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub const LATENCY_BUCKETS_US: [u64; 16] = [
    100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
    1_000_000, 2_500_000, 5_000_000, 10_000_000,
];

pub struct LatencyHistogram {
    buckets: [AtomicU64; LATENCY_BUCKETS_US.len() + 1],
    count: AtomicU64,
    sum_us: AtomicU64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct HistogramSnapshot {
    pub buckets: Vec<u64>,
    pub count: u64,
    pub sum_us: u64,
}

pub struct LatencyTimer {
    histogram: Arc<LatencyHistogram>,
    started: Instant,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum_us: AtomicU64::new(0),
        }
    }
}

impl LatencyHistogram {
    pub fn observe(&self, duration: Duration) {
        let micros = duration.as_micros().min(u64::MAX as u128) as u64;
        let bucket = LATENCY_BUCKETS_US
            .iter()
            .position(|limit| micros <= *limit)
            .unwrap_or(LATENCY_BUCKETS_US.len());
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(micros, Ordering::Relaxed);
    }

    pub fn timer(self: &Arc<Self>) -> LatencyTimer {
        LatencyTimer {
            histogram: Arc::clone(self),
            started: Instant::now(),
        }
    }

    pub fn snapshot(&self) -> HistogramSnapshot {
        HistogramSnapshot {
            buckets: self
                .buckets
                .iter()
                .map(|bucket| bucket.load(Ordering::Relaxed))
                .collect(),
            count: self.count.load(Ordering::Relaxed),
            sum_us: self.sum_us.load(Ordering::Relaxed),
        }
    }
}

impl Drop for LatencyTimer {
    fn drop(&mut self) {
        self.histogram.observe(self.started.elapsed());
    }
}

pub fn render_prometheus(name: &str, help: &str, snapshot: &HistogramSnapshot) -> String {
    let mut output = format!("# HELP {name} {help}\n# TYPE {name} histogram\n");
    let mut cumulative = 0u64;
    for (limit, count) in LATENCY_BUCKETS_US.iter().zip(&snapshot.buckets) {
        cumulative = cumulative.saturating_add(*count);
        output.push_str(&format!(
            "{name}_bucket{{le=\"{}\"}} {cumulative}\n",
            *limit as f64 / 1_000_000.0
        ));
    }
    cumulative = cumulative.saturating_add(
        snapshot
            .buckets
            .get(LATENCY_BUCKETS_US.len())
            .copied()
            .unwrap_or_default(),
    );
    output.push_str(&format!(
        "{name}_bucket{{le=\"+Inf\"}} {cumulative}\n{name}_sum {}\n{name}_count {}\n",
        snapshot.sum_us as f64 / 1_000_000.0,
        snapshot.count,
    ));
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_cumulative_prometheus_buckets() {
        let histogram = LatencyHistogram::default();
        histogram.observe(Duration::from_micros(90));
        histogram.observe(Duration::from_micros(300));
        histogram.observe(Duration::from_secs(20));
        let rendered = render_prometheus("work_seconds", "work", &histogram.snapshot());
        assert!(rendered.contains("work_seconds_bucket{le=\"0.0001\"} 1"));
        assert!(rendered.contains("work_seconds_bucket{le=\"0.0005\"} 2"));
        assert!(rendered.contains("work_seconds_bucket{le=\"+Inf\"} 3"));
        assert!(rendered.contains("work_seconds_count 3"));
    }
}
