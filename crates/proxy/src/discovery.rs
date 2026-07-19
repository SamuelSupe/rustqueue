use crate::backend::{Backend, BackendPool};
use crate::metrics::ProxyMetrics;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

const MAX_HEAD_BYTES: usize = 64 * 1024;
const MAX_NODES_BYTES: usize = 4 * 1024 * 1024;
const FULL_REFRESH_INTERVAL: Duration = Duration::from_secs(3);

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
    full_at: Instant,
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
            Ok(response) => match read_json_bounded::<HeadResponse>(response, MAX_HEAD_BYTES).await
            {
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
    let now = Instant::now();
    if let (Some(head), Some(source)) = (&head, sources.get_mut(address)) {
        if source_is_current(source, head.revision, now) {
            source.seen_at = now;
            return;
        }
    }
    let url = format!("{address}/v1/publishers");
    let nodes = match client.get(url).send().await {
        Ok(response) => match response.error_for_status() {
            Ok(response) => {
                match read_json_bounded::<NodesResponse>(response, MAX_NODES_BYTES).await {
                    Ok(nodes) => nodes,
                    Err(error) => {
                        tracing::debug!(%error, "discovery response was invalid");
                        return;
                    }
                }
            }
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
            seen_at: now,
            full_at: now,
        },
    );
}

fn source_is_current(source: &Source, revision: u64, now: Instant) -> bool {
    source.revision == revision
        && now.saturating_duration_since(source.full_at) < FULL_REFRESH_INTERVAL
}

async fn read_json_bounded<T: serde::de::DeserializeOwned>(
    mut response: reqwest::Response,
    maximum: usize,
) -> anyhow::Result<T> {
    if response
        .content_length()
        .is_some_and(|length| length > maximum as u64)
    {
        anyhow::bail!("response body exceeds {maximum} bytes");
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if bytes.len().saturating_add(chunk.len()) > maximum {
            anyhow::bail!("response body exceeds {maximum} bytes");
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(now: Instant) -> Source {
        Source {
            revision: 7,
            producers: Vec::new(),
            seen_at: now,
            full_at: now,
        }
    }

    #[test]
    fn matching_revision_is_periodically_refreshed_in_full() {
        let now = Instant::now();
        let source = source(now);

        assert!(source_is_current(
            &source,
            7,
            now + FULL_REFRESH_INTERVAL - Duration::from_millis(1)
        ));
        assert!(!source_is_current(&source, 7, now + FULL_REFRESH_INTERVAL));
        assert!(!source_is_current(&source, 8, now));
    }
}
