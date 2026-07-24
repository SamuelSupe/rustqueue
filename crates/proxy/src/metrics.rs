use rustqueue_telemetry::{render_prometheus, LatencyHistogram};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone, Default)]
pub struct ProxyMetrics {
    pub backend: Arc<LatencyHistogram>,
    pub discovery_poll: Arc<LatencyHistogram>,
    pub tcp_connection_rotations: Arc<AtomicU64>,
    pub producer_retries: Arc<AtomicU64>,
    pub producer_ambiguous_failures: Arc<AtomicU64>,
}

impl ProxyMetrics {
    pub fn render(&self) -> String {
        let mut output = render_prometheus(
            "rustqueue_proxy_backend_duration_seconds",
            "Broker HTTP request and TCP connect latency observed by the proxy.",
            &self.backend.snapshot(),
        );
        output.push_str(&render_prometheus(
            "rustqueue_proxy_discovery_poll_duration_seconds",
            "Time spent polling configured discovery endpoints.",
            &self.discovery_poll.snapshot(),
        ));
        output.push_str(
            "# HELP rustqueue_proxy_tcp_connection_rotations_total TCP tunnels closed after reaching the configured maximum age.\n\
# TYPE rustqueue_proxy_tcp_connection_rotations_total counter\n",
        );
        output.push_str(&format!(
            "rustqueue_proxy_tcp_connection_rotations_total {}\n",
            self.tcp_connection_rotations.load(Ordering::Relaxed)
        ));
        output.push_str(
            "# HELP rustqueue_proxy_producer_retries_total Publish commands retried after an explicit pre-commit Broker rejection.\n\
# TYPE rustqueue_proxy_producer_retries_total counter\n",
        );
        output.push_str(&format!(
            "rustqueue_proxy_producer_retries_total {}\n",
            self.producer_retries.load(Ordering::Relaxed)
        ));
        output.push_str(
            "# HELP rustqueue_proxy_producer_ambiguous_failures_total Publish commands failed without retry because the Broker commit outcome was ambiguous.\n\
# TYPE rustqueue_proxy_producer_ambiguous_failures_total counter\n",
        );
        output.push_str(&format!(
            "rustqueue_proxy_producer_ambiguous_failures_total {}\n",
            self.producer_ambiguous_failures.load(Ordering::Relaxed)
        ));
        output
    }
}
