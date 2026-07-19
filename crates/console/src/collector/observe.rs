use super::*;
use futures::{stream, StreamExt};
use std::time::Instant;

const MAX_HEAD_BYTES: usize = 256 * 1024;
const MAX_OBSERVATION_BYTES: usize = 32 * 1024 * 1024;

pub(super) type ObservationCache = BTreeMap<String, CachedObservation>;

#[derive(Clone)]
pub(super) struct CachedObservation {
    pod_uid: String,
    pod_ip: String,
    fence_revision: String,
    full_at: Instant,
    observation: BrokerObservation,
}

struct ObservedBroker {
    broker: BrokerView,
    cache: Option<CachedObservation>,
}

impl Collector {
    pub(super) async fn observe_brokers(
        &self,
        brokers: Vec<BrokerView>,
        managed: &ManagedResources,
    ) -> Result<Vec<BrokerView>> {
        let token = self.console_token().await?;
        let cached = self.observation_cache.lock().await.clone();
        let port = self.config.broker_http_port;
        let http = self.http.clone();
        let management_enabled = self.config.management_enabled;
        let refresh_interval = self.config.catalog_refresh_interval;
        let fences = managed.fences();
        let fence_revision = fences.revision.clone();
        let mut observed = stream::iter(brokers)
            .map(|broker| {
                let http = http.clone();
                let token = Arc::clone(&token);
                let fences = fences.clone();
                let cached = cached.get(&broker.name).cloned();
                let fence_revision = fence_revision.clone();
                async move {
                    observe_one(
                        broker,
                        cached,
                        &http,
                        &token,
                        port,
                        management_enabled,
                        refresh_interval,
                        fences,
                        fence_revision,
                    )
                    .await
                }
            })
            .buffer_unordered(32)
            .collect::<Vec<_>>()
            .await;
        observed.sort_by(|left, right| left.broker.name.cmp(&right.broker.name));

        let active: BTreeSet<_> = observed
            .iter()
            .map(|item| item.broker.name.clone())
            .collect();
        let mut cache = self.observation_cache.lock().await;
        cache.retain(|name, _| active.contains(name));
        for item in &observed {
            if let Some(value) = &item.cache {
                cache.insert(item.broker.name.clone(), value.clone());
            }
        }
        Ok(observed.into_iter().map(|item| item.broker).collect())
    }

    async fn console_token(&self) -> Result<Arc<str>> {
        let token = tokio::fs::read_to_string(&self.config.console_token_file)
            .await
            .with_context(|| {
                format!(
                    "read console token {}",
                    self.config.console_token_file.display()
                )
            })?;
        let token = Arc::<str>::from(token.trim());
        if token.is_empty() {
            anyhow::bail!("console token is empty");
        }
        Ok(token)
    }
}

#[allow(clippy::too_many_arguments)]
async fn observe_one(
    mut broker: BrokerView,
    cached: Option<CachedObservation>,
    http: &reqwest::Client,
    token: &str,
    port: u16,
    management_enabled: bool,
    refresh_interval: std::time::Duration,
    fences: rustqueue_queue::ManagementFenceSnapshot,
    fence_revision: String,
) -> ObservedBroker {
    if broker.pod_ip.is_empty() {
        broker.error = Some("Pod has no IP address".into());
        return ObservedBroker {
            broker,
            cache: None,
        };
    }
    let result = fetch_observation(
        &broker,
        cached,
        http,
        token,
        port,
        management_enabled,
        refresh_interval,
        &fences,
        &fence_revision,
    )
    .await;
    match result {
        Ok(value) => {
            broker.observation = Some(value.observation.clone());
            ObservedBroker {
                broker,
                cache: Some(value),
            }
        }
        Err(error) => {
            broker.error = Some(error.to_string());
            ObservedBroker {
                broker,
                cache: None,
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn fetch_observation(
    broker: &BrokerView,
    cached: Option<CachedObservation>,
    http: &reqwest::Client,
    token: &str,
    port: u16,
    management_enabled: bool,
    refresh_interval: std::time::Duration,
    fences: &rustqueue_queue::ManagementFenceSnapshot,
    fence_revision: &str,
) -> Result<CachedObservation> {
    let origin = format!("http://{}:{port}", broker.pod_ip);
    let head_response = http
        .get(format!("{origin}/v1/observe/head"))
        .bearer_auth(token)
        .send()
        .await?
        .error_for_status()?;
    let head = read_json_bounded(head_response, MAX_HEAD_BYTES).await?;
    let requires_full = requires_full(
        cached.as_ref(),
        broker,
        &head,
        fence_revision,
        refresh_interval,
    );
    let requires_fence_sync = requires_fence_sync(cached.as_ref(), broker, fence_revision);
    if !requires_full {
        let mut value = cached.expect("cached observation was checked");
        head.merge_into(&mut value.observation);
        return Ok(value);
    }
    if management_enabled && requires_fence_sync {
        http.post(format!("{origin}/v1/manage/fences/sync"))
            .bearer_auth(token)
            .json(fences)
            .send()
            .await?
            .error_for_status()?;
    }
    let observation_response = http
        .get(format!("{origin}/v1/observe"))
        .bearer_auth(token)
        .send()
        .await?
        .error_for_status()?;
    let observation = read_json_bounded(observation_response, MAX_OBSERVATION_BYTES).await?;
    Ok(CachedObservation {
        pod_uid: broker.uid.clone(),
        pod_ip: broker.pod_ip.clone(),
        fence_revision: fence_revision.to_owned(),
        full_at: Instant::now(),
        observation,
    })
}

fn requires_fence_sync(
    cached: Option<&CachedObservation>,
    broker: &BrokerView,
    fence_revision: &str,
) -> bool {
    cached.is_none_or(|value| {
        value.pod_uid != broker.uid
            || value.pod_ip != broker.pod_ip
            || value.fence_revision != fence_revision
    })
}

async fn read_json_bounded<T: serde::de::DeserializeOwned>(
    mut response: reqwest::Response,
    maximum: usize,
) -> Result<T> {
    if response
        .content_length()
        .is_some_and(|length| length > maximum as u64)
    {
        anyhow::bail!("broker response exceeds {maximum} bytes");
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if bytes.len().saturating_add(chunk.len()) > maximum {
            anyhow::bail!("broker response exceeds {maximum} bytes");
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&bytes)?)
}

fn requires_full(
    cached: Option<&CachedObservation>,
    broker: &BrokerView,
    head: &BrokerObservationHead,
    fence_revision: &str,
    refresh_interval: std::time::Duration,
) -> bool {
    cached.is_none_or(|value| {
        value.pod_uid != broker.uid
            || value.pod_ip != broker.pod_ip
            || value.fence_revision != fence_revision
            || value.observation.registry_revision != head.registry_revision
            || value.observation.node.version != head.node.version
            || value.full_at.elapsed() >= refresh_interval
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (BrokerView, BrokerObservationHead, CachedObservation) {
        let broker = BrokerView {
            uid: "pod-1".into(),
            name: "queue-0".into(),
            pod_ip: "10.0.0.7".into(),
            ..Default::default()
        };
        let mut observation = BrokerObservation {
            registry_revision: 9,
            ..Default::default()
        };
        observation.node.version = "0.7.1".into();
        let mut head = BrokerObservationHead {
            registry_revision: 9,
            ..Default::default()
        };
        head.node.version = "0.7.1".into();
        let cached = CachedObservation {
            pod_uid: broker.uid.clone(),
            pod_ip: broker.pod_ip.clone(),
            fence_revision: "fence-1".into(),
            full_at: Instant::now(),
            observation,
        };
        (broker, head, cached)
    }

    #[test]
    fn stable_head_reuses_the_cached_catalog() {
        let (broker, head, cached) = fixture();
        assert!(!requires_full(
            Some(&cached),
            &broker,
            &head,
            "fence-1",
            std::time::Duration::from_secs(30),
        ));
    }

    #[test]
    fn topology_or_fence_change_forces_a_full_catalog_refresh() {
        let (broker, mut head, cached) = fixture();
        head.registry_revision += 1;
        assert!(requires_full(
            Some(&cached),
            &broker,
            &head,
            "fence-1",
            std::time::Duration::from_secs(30),
        ));
        head.registry_revision -= 1;
        assert!(requires_full(
            Some(&cached),
            &broker,
            &head,
            "fence-2",
            std::time::Duration::from_secs(30),
        ));
    }

    #[test]
    fn replacement_pod_requires_management_fence_resynchronization() {
        let (mut broker, _, cached) = fixture();
        assert!(!requires_fence_sync(Some(&cached), &broker, "fence-1"));

        broker.uid = "pod-2".into();
        assert!(requires_fence_sync(Some(&cached), &broker, "fence-1"));
        assert!(requires_fence_sync(Some(&cached), &broker, "fence-2"));
        assert!(requires_fence_sync(None, &broker, "fence-1"));
    }
}
