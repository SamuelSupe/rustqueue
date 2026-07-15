mod health;
mod kube_resources;
mod nodes;
mod reconcile;
mod reconcile_state;
mod rollout;
mod security;
mod storage;

use crate::crd::RustQueueCluster;
use futures::StreamExt;
use k8s_openapi::api::apps::v1::StatefulSet;
use kube::runtime::controller::{Action, Controller};
use kube::runtime::watcher;
use kube::{Api, Client, ResourceExt};
use std::sync::Arc;
use std::time::Duration;

pub(super) struct Context {
    pub client: Client,
    pub namespace: String,
    pub http: reqwest::Client,
    pub health: Arc<health::HealthState>,
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub(super) struct ReconcileError(#[from] anyhow::Error);

pub async fn run() -> anyhow::Result<()> {
    let client = Client::try_default().await?;
    let namespace = watch_namespace();
    let health = Arc::new(health::HealthState::default());
    health::spawn(Arc::clone(&health)).await?;
    let context = Arc::new(Context {
        client: client.clone(),
        namespace: namespace.clone(),
        http: reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(10))
            .build()?,
        health,
    });
    let clusters = Api::<RustQueueCluster>::namespaced(client.clone(), &namespace);
    let stateful_sets = Api::<StatefulSet>::namespaced(client, &namespace);

    tracing::info!(%namespace, "RustQueue Kubernetes Operator started");
    Controller::new(clusters, watcher::Config::default())
        .owns(stateful_sets, watcher::Config::default())
        .run(reconcile::reconcile, error_policy, context)
        .for_each(|result| async move {
            match result {
                Ok((reference, _)) => {
                    tracing::debug!(object = %reference.name, "reconciliation completed")
                }
                Err(error) => tracing::error!(%error, "controller stream error"),
            }
        })
        .await;
    Ok(())
}

fn error_policy(
    cluster: Arc<RustQueueCluster>,
    error: &ReconcileError,
    context: Arc<Context>,
) -> Action {
    context.health.record_error();
    tracing::warn!(cluster = %cluster.name_any(), %error, "reconciliation failed; retrying");
    Action::requeue(Duration::from_secs(10))
}

fn watch_namespace() -> String {
    std::env::var("WATCH_NAMESPACE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            std::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace").ok()
        })
        .map(|value| value.trim().to_owned())
        .unwrap_or_else(|| "default".into())
}
