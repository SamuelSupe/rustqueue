use crate::backend::{Backend, BackendPool};
use crate::metrics::ProxyMetrics;
use serde::Deserialize;
use std::time::Duration;

#[derive(Deserialize)]
struct NodesResponse {
    producers: Vec<Backend>,
}

pub async fn run(
    pool: BackendPool,
    addresses: Vec<String>,
    metrics: ProxyMetrics,
) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_millis(500))
        .timeout(Duration::from_millis(1500))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let mut interval = tokio::time::interval(Duration::from_secs(2));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let _poll_timer = metrics.discovery_poll.timer();
        let mut discovered = Vec::new();
        let mut successful = false;
        for address in &addresses {
            let url = format!("{}/v1/publishers", address.trim_end_matches('/'));
            match client.get(url).send().await {
                Ok(response) => match response.error_for_status() {
                    Ok(response) => match response.json::<NodesResponse>().await {
                        Ok(nodes) => {
                            successful = true;
                            discovered.extend(nodes.producers);
                        }
                        Err(error) => tracing::debug!(%error, "discovery response was invalid"),
                    },
                    Err(error) => tracing::debug!(%error, "discovery returned an error"),
                },
                Err(error) => tracing::debug!(%error, "discovery request failed"),
            }
        }
        if successful {
            pool.replace(discovered);
        } else {
            pool.clear_if_stale(Duration::from_secs(5));
        }
    }
}
