use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

#[derive(Clone, Copy)]
pub(crate) enum RpcKind {
    Append,
    Snapshot,
    Vote,
    Write,
    Fetch,
    Ack,
    Control,
}

#[derive(Default)]
struct Counters {
    requests: AtomicU64,
    request_bytes: AtomicU64,
    response_bytes: AtomicU64,
}

const BATCH_BUCKETS: [u64; 7] = [1, 2, 4, 8, 16, 32, 64];

#[derive(Default)]
struct BatchHistogram {
    buckets: [AtomicU64; BATCH_BUCKETS.len()],
    count: AtomicU64,
    sum: AtomicU64,
}

impl BatchHistogram {
    fn observe(&self, value: usize) {
        let value = value as u64;
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value, Ordering::Relaxed);
        for (upper, bucket) in BATCH_BUCKETS.iter().zip(&self.buckets) {
            if value <= *upper {
                bucket.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn render(&self, name: &str) -> String {
        let count = self.count.load(Ordering::Relaxed);
        let mut output = format!("# TYPE {name} histogram\n");
        for (upper, bucket) in BATCH_BUCKETS.iter().zip(&self.buckets) {
            output.push_str(&format!(
                "{name}_bucket{{le=\"{upper}\"}} {}\n",
                bucket.load(Ordering::Relaxed)
            ));
        }
        output.push_str(&format!(
            "{name}_bucket{{le=\"+Inf\"}} {count}\n{name}_sum {}\n{name}_count {count}\n",
            self.sum.load(Ordering::Relaxed)
        ));
        output
    }
}

#[derive(Default)]
pub(crate) struct NetworkMetrics {
    append: Counters,
    snapshot: Counters,
    vote: Counters,
    write: Counters,
    fetch: Counters,
    ack: Counters,
    control: Counters,
    empty_fetches: AtomicU64,
    redirects: AtomicU64,
    retries: AtomicU64,
    fetch_batches: AtomicU64,
    fetch_messages: AtomicU64,
    fetch_bytes: AtomicU64,
    ack_batches: AtomicU64,
    ack_messages: AtomicU64,
    fetch_batch_size: BatchHistogram,
    ack_batch_size: BatchHistogram,
}

static METRICS: OnceLock<NetworkMetrics> = OnceLock::new();

pub(crate) fn network_metrics() -> &'static NetworkMetrics {
    METRICS.get_or_init(NetworkMetrics::default)
}

impl NetworkMetrics {
    fn counters(&self, kind: RpcKind) -> &Counters {
        match kind {
            RpcKind::Append => &self.append,
            RpcKind::Snapshot => &self.snapshot,
            RpcKind::Vote => &self.vote,
            RpcKind::Write => &self.write,
            RpcKind::Fetch => &self.fetch,
            RpcKind::Ack => &self.ack,
            RpcKind::Control => &self.control,
        }
    }

    pub(crate) fn record_request(&self, kind: RpcKind, bytes: usize) {
        let counters = self.counters(kind);
        counters.requests.fetch_add(1, Ordering::Relaxed);
        counters
            .request_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_response(&self, kind: RpcKind, bytes: usize) {
        self.counters(kind)
            .response_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_fetch(&self, messages: usize, bytes: usize) {
        self.fetch_batches.fetch_add(1, Ordering::Relaxed);
        self.fetch_batch_size.observe(messages);
        self.fetch_messages
            .fetch_add(messages as u64, Ordering::Relaxed);
        self.fetch_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        if messages == 0 {
            self.empty_fetches.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_ack_batch(&self, messages: usize) {
        self.ack_batches.fetch_add(1, Ordering::Relaxed);
        self.ack_batch_size.observe(messages);
        self.ack_messages
            .fetch_add(messages as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_redirect(&self) {
        self.redirects.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_retry(&self) {
        self.retries.fetch_add(1, Ordering::Relaxed);
    }
}

pub fn render_network_metrics() -> String {
    let metrics = network_metrics();
    let mut output = String::from(
        "# TYPE rustqueue_internal_rpc_requests_total counter\n\
         # TYPE rustqueue_internal_rpc_request_bytes_total counter\n\
         # TYPE rustqueue_internal_rpc_response_bytes_total counter\n",
    );
    for (name, counters) in [
        ("append", &metrics.append),
        ("snapshot", &metrics.snapshot),
        ("vote", &metrics.vote),
        ("write", &metrics.write),
        ("fetch", &metrics.fetch),
        ("ack", &metrics.ack),
        ("control", &metrics.control),
    ] {
        output.push_str(&format!(
            "rustqueue_internal_rpc_requests_total{{operation=\"{name}\"}} {}\n\
             rustqueue_internal_rpc_request_bytes_total{{operation=\"{name}\"}} {}\n\
             rustqueue_internal_rpc_response_bytes_total{{operation=\"{name}\"}} {}\n",
            counters.requests.load(Ordering::Relaxed),
            counters.request_bytes.load(Ordering::Relaxed),
            counters.response_bytes.load(Ordering::Relaxed),
        ));
    }
    output.push_str(
        &metrics
            .fetch_batch_size
            .render("rustqueue_fetch_batch_messages"),
    );
    output.push_str(
        &metrics
            .ack_batch_size
            .render("rustqueue_ack_batch_messages"),
    );
    output.push_str(&format!(
        "# TYPE rustqueue_fetch_empty_total counter\n\
         rustqueue_fetch_empty_total {}\n\
         # TYPE rustqueue_leader_redirects_total counter\n\
         rustqueue_leader_redirects_total {}\n\
         # TYPE rustqueue_internal_rpc_retries_total counter\n\
         rustqueue_internal_rpc_retries_total {}\n\
         # TYPE rustqueue_fetch_batches_total counter\n\
         rustqueue_fetch_batches_total {}\n\
         # TYPE rustqueue_fetch_messages_total counter\n\
         rustqueue_fetch_messages_total {}\n\
         # TYPE rustqueue_fetch_bytes_total counter\n\
         rustqueue_fetch_bytes_total {}\n\
         # TYPE rustqueue_ack_batches_total counter\n\
         rustqueue_ack_batches_total {}\n\
         # TYPE rustqueue_ack_messages_total counter\n\
         rustqueue_ack_messages_total {}\n",
        metrics.empty_fetches.load(Ordering::Relaxed),
        metrics.redirects.load(Ordering::Relaxed),
        metrics.retries.load(Ordering::Relaxed),
        metrics.fetch_batches.load(Ordering::Relaxed),
        metrics.fetch_messages.load(Ordering::Relaxed),
        metrics.fetch_bytes.load(Ordering::Relaxed),
        metrics.ack_batches.load(Ordering::Relaxed),
        metrics.ack_messages.load(Ordering::Relaxed),
    ));
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_histogram_is_cumulative_and_reports_sum() {
        let histogram = BatchHistogram::default();
        histogram.observe(3);
        histogram.observe(64);
        let metrics = histogram.render("batch");
        assert!(metrics.contains("batch_bucket{le=\"2\"} 0"));
        assert!(metrics.contains("batch_bucket{le=\"4\"} 1"));
        assert!(metrics.contains("batch_bucket{le=\"64\"} 2"));
        assert!(metrics.contains("batch_sum 67"));
        assert!(metrics.contains("batch_count 2"));
    }
}
