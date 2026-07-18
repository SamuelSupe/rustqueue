use rustqueue_telemetry::{render_prometheus, LatencyHistogram};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone, Default)]
pub struct DiscoveryMetrics {
    pub registry_poll: Arc<LatencyHistogram>,
    pub refresh: Arc<LatencyHistogram>,
    pub endpoint_slice_timeouts: Arc<AtomicU64>,
}

impl DiscoveryMetrics {
    pub fn render(&self) -> String {
        let mut output = render_prometheus(
            "rustqueue_discovery_registry_poll_duration_seconds",
            "Latency of one broker registry poll.",
            &self.registry_poll.snapshot(),
        );
        output.push_str(&render_prometheus(
            "rustqueue_discovery_refresh_duration_seconds",
            "Latency of one EndpointSlice and broker registry refresh cycle.",
            &self.refresh.snapshot(),
        ));
        output.push_str(
            "# HELP rustqueue_discovery_endpoint_slice_timeouts_total Kubernetes EndpointSlice list requests that exceeded their deadline.\n\
# TYPE rustqueue_discovery_endpoint_slice_timeouts_total counter\n",
        );
        output.push_str(&format!(
            "rustqueue_discovery_endpoint_slice_timeouts_total {}\n",
            self.endpoint_slice_timeouts.load(Ordering::Relaxed)
        ));
        output
    }
}
