use rustqueue_telemetry::{render_prometheus, LatencyHistogram};
use std::sync::Arc;

#[derive(Clone, Default)]
pub struct DiscoveryMetrics {
    pub registry_poll: Arc<LatencyHistogram>,
    pub refresh: Arc<LatencyHistogram>,
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
        output
    }
}
