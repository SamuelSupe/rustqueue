use crate::backend::{Backend, BackendPool};
use crate::metrics::ProxyMetrics;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

#[derive(Deserialize)]
struct NodesResponse {
    #[serde(default)]
    revision: u64,
    producers: Vec<Backend>,
}

#[derive(Deserialize)]
struct HeadResponse {
    revision: u64,
}

struct Source {
    revision: u64,
    producers: Vec<Backend>,
    seen_at: Instant,
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
    let mut sources = BTreeMap::<String, Source>::new();
    let mut interval = tokio::time::interval(Duration::from_secs(2));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let _poll_timer = metrics.discovery_poll.timer();
        for address in &addresses {
            refresh_source(&client, address, &mut sources).await;
        }
        sources.retain(|_, source| source.seen_at.elapsed() <= Duration::from_secs(5));
        if sources.is_empty() {
            pool.clear_if_stale(Duration::from_secs(5));
        } else {
            pool.replace(
                sources
                    .values()
                    .flat_map(|source| source.producers.iter().cloned())
                    .collect(),
            );
        }
    }
}

async fn refresh_source(
    client: &reqwest::Client,
    address: &str,
    sources: &mut BTreeMap<String, Source>,
) {
    let address = address.trim_end_matches('/');
    let head_url = format!("{address}/v1/publishers/head");
    let head = match client.get(head_url).send().await {
        Ok(response) if response.status() == reqwest::StatusCode::NOT_FOUND => None,
        Ok(response) => match response.error_for_status() {
            Ok(response) => match response.json::<HeadResponse>().await {
                Ok(head) => Some(head),
                Err(error) => {
                    tracing::debug!(%error, "discovery head response was invalid");
                    return;
                }
            },
            Err(error) => {
                tracing::debug!(%error, "discovery head returned an error");
                return;
            }
        },
        Err(error) => {
            tracing::debug!(%error, "discovery head request failed");
            return;
        }
    };
    if let (Some(head), Some(source)) = (&head, sources.get_mut(address)) {
        if source.revision == head.revision {
            source.seen_at = Instant::now();
            return;
        }
    }
    let url = format!("{address}/v1/publishers");
    let nodes = match client.get(url).send().await {
        Ok(response) => match response.error_for_status() {
            Ok(response) => match response.json::<NodesResponse>().await {
                Ok(nodes) => nodes,
                Err(error) => {
                    tracing::debug!(%error, "discovery response was invalid");
                    return;
                }
            },
            Err(error) => {
                tracing::debug!(%error, "discovery returned an error");
                return;
            }
        },
        Err(error) => {
            tracing::debug!(%error, "discovery request failed");
            return;
        }
    };
    sources.insert(
        address.to_owned(),
        Source {
            revision: head.map_or(nodes.revision, |head| head.revision),
            producers: nodes.producers,
            seen_at: Instant::now(),
        },
    );
}
