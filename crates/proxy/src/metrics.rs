use rustqueue_telemetry::{render_prometheus, LatencyHistogram};
use std::sync::Arc;

#[derive(Clone, Default)]
pub struct ProxyMetrics {
    pub backend: Arc<LatencyHistogram>,
    pub discovery_poll: Arc<LatencyHistogram>,
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
        output
    }
}
