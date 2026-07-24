use crate::backend::{Backend, BackendPool};
use crate::metrics::ProxyMetrics;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

const MAX_HEAD_BYTES: usize = 64 * 1024;
const MAX_NODES_BYTES: usize = 4 * 1024 * 1024;
const FULL_REFRESH_INTERVAL: Duration = Duration::from_secs(3);
const ROUTING_STALE_AFTER: Duration = Duration::from_secs(5);

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
    brokers: Vec<Backend>,
    seen_at: Instant,
    full_at: Instant,
}

pub async fn run(
    publish_pool: BackendPool,
    broker_pool: BackendPool,
    addresses: Vec<String>,
    metrics: ProxyMetrics,
) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_millis(500))
        .timeout(Duration::from_millis(1500))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let mut sources = BTreeMap::<String, Source>::new();
    let mut last_coherent_at = None;
    let mut interval = tokio::time::interval(Duration::from_secs(2));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let _poll_timer = metrics.discovery_poll.timer();
        for address in &addresses {
            refresh_source(&client, address, &mut sources).await;
        }
        sources.retain(|_, source| source.seen_at.elapsed() <= ROUTING_STALE_AFTER);
        match coherent_sources(&sources) {
            Some((producers, brokers, observed_at)) => {
                publish_pool.replace(producers);
                broker_pool.replace(brokers);
                last_coherent_at = Some(observed_at);
            }
            None if last_coherent_is_fresh(last_coherent_at, Instant::now()) => {
                tracing::debug!(
                    "retaining the last coherent routing snapshot during Discovery revision skew"
                );
            }
            None => {
                if !sources.is_empty() {
                    tracing::debug!(
                        "active discovery replicas returned different routing revisions"
                    );
                }
                publish_pool.replace(Vec::new());
                broker_pool.replace(Vec::new());
            }
        }
    }
}

fn coherent_sources(
    sources: &BTreeMap<String, Source>,
) -> Option<(Vec<Backend>, Vec<Backend>, Instant)> {
    let revision = sources.values().next()?.revision;
    if sources.values().any(|source| source.revision != revision) {
        return None;
    }
    let observed_at = sources.values().map(|source| source.seen_at).min()?;
    Some((
        sources
            .values()
            .flat_map(|source| source.producers.iter().cloned())
            .collect(),
        sources
            .values()
            .flat_map(|source| source.brokers.iter().cloned())
            .collect(),
        observed_at,
    ))
}

fn last_coherent_is_fresh(last_coherent_at: Option<Instant>, now: Instant) -> bool {
    last_coherent_at.is_some_and(|observed_at| {
        now.saturating_duration_since(observed_at) <= ROUTING_STALE_AFTER
    })
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
    let nodes = match fetch_nodes(client, &format!("{address}/v1/publishers")).await {
        Ok(nodes) => nodes,
        Err(error) => {
            tracing::debug!(%error, "discovery publishers response was invalid");
            return;
        }
    };
    let broker_nodes = match fetch_optional_nodes(client, &format!("{address}/v1/brokers")).await {
        Ok(nodes) => nodes,
        Err(error) => {
            tracing::debug!(%error, "discovery brokers response was invalid");
            return;
        }
    };
    let Some(revision) = coherent_revision(head.as_ref(), &nodes, broker_nodes.as_ref()) else {
        tracing::debug!("discovery routing snapshot revisions did not match");
        return;
    };
    let brokers = broker_nodes
        .map(|nodes| nodes.producers)
        .unwrap_or_else(|| nodes.producers.clone());
    sources.insert(
        address.to_owned(),
        Source {
            revision,
            producers: nodes.producers,
            brokers,
            seen_at: now,
            full_at: now,
        },
    );
}

async fn fetch_nodes(client: &reqwest::Client, url: &str) -> anyhow::Result<NodesResponse> {
    let response = client.get(url).send().await?.error_for_status()?;
    read_json_bounded(response, MAX_NODES_BYTES).await
}

async fn fetch_optional_nodes(
    client: &reqwest::Client,
    url: &str,
) -> anyhow::Result<Option<NodesResponse>> {
    let response = client.get(url).send().await?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    Ok(Some(
        read_json_bounded(response.error_for_status()?, MAX_NODES_BYTES).await?,
    ))
}

fn source_is_current(source: &Source, revision: u64, now: Instant) -> bool {
    source.revision == revision
        && now.saturating_duration_since(source.full_at) < FULL_REFRESH_INTERVAL
}

fn coherent_revision(
    head: Option<&HeadResponse>,
    publishers: &NodesResponse,
    brokers: Option<&NodesResponse>,
) -> Option<u64> {
    let revision = head.map_or(publishers.revision, |head| head.revision);
    (publishers.revision == revision && brokers.is_none_or(|brokers| brokers.revision == revision))
        .then_some(revision)
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
            brokers: Vec::new(),
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

    #[test]
    fn mixed_discovery_revisions_are_rejected() {
        let publishers = NodesResponse {
            revision: 7,
            producers: Vec::new(),
        };
        let brokers = NodesResponse {
            revision: 8,
            producers: Vec::new(),
        };
        assert_eq!(
            coherent_revision(
                Some(&HeadResponse { revision: 7 }),
                &publishers,
                Some(&brokers)
            ),
            None
        );
        assert_eq!(
            coherent_revision(Some(&HeadResponse { revision: 7 }), &publishers, None),
            Some(7)
        );
    }

    #[test]
    fn active_discovery_replicas_must_agree_before_routing() {
        let now = Instant::now();
        let mut sources = BTreeMap::from([
            ("first".into(), source(now)),
            ("second".into(), source(now)),
        ]);
        assert!(coherent_sources(&sources).is_some());

        sources.get_mut("second").unwrap().revision = 8;
        assert!(coherent_sources(&sources).is_none());

        sources.remove("second");
        assert!(coherent_sources(&sources).is_some());
    }

    #[test]
    fn last_coherent_routing_only_bridges_bounded_revision_skew() {
        let now = Instant::now();
        assert!(last_coherent_is_fresh(Some(now), now + ROUTING_STALE_AFTER));
        assert!(!last_coherent_is_fresh(
            Some(now),
            now + ROUTING_STALE_AFTER + Duration::from_millis(1)
        ));
        assert!(!last_coherent_is_fresh(None, now));
    }
}
