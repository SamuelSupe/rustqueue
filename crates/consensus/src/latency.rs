use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const BOUNDS_US: [u64; 13] = [
    250, 500, 1_000, 2_000, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000,
    5_000_000,
];
const BUCKET_COUNT: usize = BOUNDS_US.len() + 1;

pub(crate) struct LatencyHistogram {
    buckets: [AtomicU64; BUCKET_COUNT],
    count: AtomicU64,
    sum_us: AtomicU64,
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
    pub(crate) fn record(&self, elapsed: Duration) {
        let micros = elapsed.as_micros().min(u64::MAX as u128) as u64;
        let bucket = BOUNDS_US
            .iter()
            .position(|bound| micros <= *bound)
            .unwrap_or(BOUNDS_US.len());
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(micros, Ordering::Relaxed);
    }

    pub(crate) fn timer(&self) -> LatencyTimer<'_> {
        LatencyTimer {
            histogram: self,
            started: Instant::now(),
        }
    }

    pub(crate) fn render(&self, name: &str, help: &str, labels: &str) -> String {
        let mut output = format!("# HELP {name} {help}\n# TYPE {name} histogram\n");
        output.push_str(&self.render_samples(name, labels));
        output
    }

    fn render_samples(&self, name: &str, labels: &str) -> String {
        let mut output = String::new();
        let mut cumulative = 0u64;
        for (index, bound) in BOUNDS_US.iter().enumerate() {
            cumulative = cumulative.saturating_add(self.buckets[index].load(Ordering::Relaxed));
            let le = *bound as f64 / 1_000_000.0;
            let _ = writeln!(output, "{name}_bucket{{{labels},le=\"{le}\"}} {cumulative}");
        }
        cumulative =
            cumulative.saturating_add(self.buckets[BOUNDS_US.len()].load(Ordering::Relaxed));
        let count = self.count.load(Ordering::Relaxed);
        let sum = self.sum_us.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let _ = writeln!(output, "{name}_bucket{{{labels},le=\"+Inf\"}} {cumulative}");
        let _ = writeln!(output, "{name}_sum{{{labels}}} {sum}");
        let _ = writeln!(output, "{name}_count{{{labels}}} {count}");
        output
    }
}

pub(crate) struct LatencyTimer<'a> {
    histogram: &'a LatencyHistogram,
    started: Instant,
}

impl Drop for LatencyTimer<'_> {
    fn drop(&mut self) {
        self.histogram.record(self.started.elapsed());
    }
}

#[derive(Default)]
pub(crate) struct GroupLatencyMetrics {
    pub(crate) fsync: LatencyHistogram,
    pub(crate) group_commit: LatencyHistogram,
    pub(crate) forward: LatencyHistogram,
    pub(crate) snapshot_build: LatencyHistogram,
    pub(crate) snapshot_install: LatencyHistogram,
    pub(crate) gc: LatencyHistogram,
}

impl GroupLatencyMetrics {
    pub(crate) fn render(&self, labels: &str, declarations: bool) -> String {
        let metrics = [
            (
                &self.fsync,
                "rustqueue_fsync_duration_seconds",
                "Raft log fsync latency in seconds.",
            ),
            (
                &self.group_commit,
                "rustqueue_group_commit_duration_seconds",
                "Quorum group commit latency in seconds.",
            ),
            (
                &self.forward,
                "rustqueue_forward_duration_seconds",
                "Internal group forwarding latency in seconds.",
            ),
            (
                &self.snapshot_build,
                "rustqueue_snapshot_build_duration_seconds",
                "Snapshot build latency in seconds.",
            ),
            (
                &self.snapshot_install,
                "rustqueue_snapshot_install_duration_seconds",
                "Snapshot install latency in seconds.",
            ),
            (
                &self.gc,
                "rustqueue_gc_duration_seconds",
                "Physical segment garbage collection latency in seconds.",
            ),
        ];
        metrics
            .into_iter()
            .map(|(histogram, name, help)| {
                if declarations {
                    histogram.render(name, help, labels)
                } else {
                    histogram.render_samples(name, labels)
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_cumulative_prometheus_buckets() {
        let histogram = LatencyHistogram::default();
        histogram.record(Duration::from_micros(300));
        histogram.record(Duration::from_millis(2));
        let rendered = histogram.render("latency", "test", "group_id=\"1\"");
        assert!(rendered.contains("latency_bucket{group_id=\"1\",le=\"0.0005\"} 1"));
        assert!(rendered.contains("latency_bucket{group_id=\"1\",le=\"+Inf\"} 2"));
        assert!(rendered.contains("latency_count{group_id=\"1\"} 2"));
    }
}
