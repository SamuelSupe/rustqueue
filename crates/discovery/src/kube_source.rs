use crate::{BrokerEndpoint, BrokerRegistry, Directory};
use anyhow::Context;
use futures::stream::{FuturesUnordered, StreamExt};
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::api::{Api, ListParams};
use kube::Client;
use std::collections::BTreeSet;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

#[derive(Clone, Debug)]
pub struct RefreshConfig {
    pub namespace: String,
    pub service_name: String,
    pub fallback_http_port: u16,
    pub poll_interval: Duration,
    pub stale_after: Duration,
    pub registry_token: Option<String>,
    pub max_parallel_polls: usize,
}

pub async fn run_refresh_loop(directory: Directory, config: RefreshConfig) -> anyhow::Result<()> {
    let client = Client::try_default()
        .await
        .context("create Kubernetes client")?;
    let api: Api<EndpointSlice> = Api::namespaced(client, &config.namespace);
    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_millis(300))
        .timeout(Duration::from_millis(750))
        .pool_idle_timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let selector = format!("kubernetes.io/service-name={}", config.service_name);
    let mut interval = tokio::time::interval(config.poll_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let _refresh_timer = directory.metrics().refresh.timer();
        match api.list(&ListParams::default().labels(&selector)).await {
            Ok(slices) => {
                let endpoints = endpoints_from_slices(&slices.items, config.fallback_http_port);
                directory.replace_endpoints(endpoints);
                poll_registries(&directory, &http, &config).await;
            }
            Err(error) => tracing::warn!(%error, "EndpointSlice refresh failed"),
        }
        directory.expire(config.stale_after);
    }
}

fn endpoints_from_slices(slices: &[EndpointSlice], fallback_port: u16) -> BTreeSet<BrokerEndpoint> {
    let mut result = BTreeSet::new();
    for slice in slices {
        let port = slice
            .ports
            .as_ref()
            .and_then(|ports| {
                ports
                    .iter()
                    .find(|port| port.name.as_deref() == Some("http"))
                    .or_else(|| ports.first())
                    .and_then(|port| port.port)
            })
            .and_then(|port| u16::try_from(port).ok())
            .unwrap_or(fallback_port);
        for endpoint in &slice.endpoints {
            if endpoint
                .conditions
                .as_ref()
                .and_then(|conditions| conditions.terminating)
                == Some(true)
            {
                continue;
            }
            result.extend(
                endpoint
                    .addresses
                    .iter()
                    .filter_map(|address| address.parse::<IpAddr>().ok())
                    .map(|address| BrokerEndpoint {
                        address,
                        http_port: port,
                    }),
            );
        }
    }
    result
}

async fn poll_registries(directory: &Directory, http: &reqwest::Client, config: &RefreshConfig) {
    let permits = Arc::new(Semaphore::new(config.max_parallel_polls.max(1)));
    let mut polls = FuturesUnordered::new();
    for endpoint in directory.endpoints() {
        let http = http.clone();
        let token = config.registry_token.clone();
        let permits = Arc::clone(&permits);
        let metrics = directory.metrics().clone();
        polls.push(async move {
            let _permit = permits.acquire_owned().await.ok()?;
            let _timer = metrics.registry_poll.timer();
            let mut request = http.get(endpoint.registry_url());
            if let Some(token) = token {
                request = request.bearer_auth(token);
            }
            let registry = request
                .send()
                .await
                .ok()?
                .error_for_status()
                .ok()?
                .json::<BrokerRegistry>()
                .await
                .ok()?;
            Some((endpoint, registry))
        });
    }
    while let Some(observation) = polls.next().await {
        if let Some((endpoint, registry)) = observation {
            directory.observe(endpoint, registry);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::discovery::v1::{Endpoint, EndpointConditions, EndpointPort};

    #[test]
    fn extracts_not_ready_brokers_for_drain_aware_registry_polling() {
        let slice = EndpointSlice {
            endpoints: vec![
                Endpoint {
                    addresses: vec!["10.0.0.1".into()],
                    conditions: Some(EndpointConditions {
                        ready: Some(true),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                Endpoint {
                    addresses: vec!["10.0.0.2".into()],
                    conditions: Some(EndpointConditions {
                        ready: Some(false),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ],
            ports: Some(vec![EndpointPort {
                name: Some("http".into()),
                port: Some(4151),
                ..Default::default()
            }]),
            ..Default::default()
        };
        let endpoints = endpoints_from_slices(&[slice], 9999);
        assert_eq!(endpoints.len(), 2);
        assert_eq!(endpoints.iter().next().unwrap().http_port, 4151);
    }
}
