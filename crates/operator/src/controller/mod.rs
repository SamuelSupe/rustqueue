mod apply;
mod auth;
mod drain;
mod nodes;
mod preflight;

use crate::crd::RustQueueStatus;
use crate::resources::{self, BuildInput};
use crate::RustQueue;
use anyhow::{bail, Context as _};
use futures::StreamExt;
use k8s_openapi::api::apps::v1::StatefulSet;
use kube::api::Api;
use kube::runtime::controller::{Action, Controller};
use kube::runtime::watcher;
use kube::{Client, ResourceExt};
use std::sync::Arc;
use std::time::Duration;

pub(super) struct ContextData {
    pub client: Client,
    pub http: reqwest::Client,
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct ReconcileError(#[from] anyhow::Error);

pub async fn run() -> anyhow::Result<()> {
    let client = Client::try_default().await?;
    let namespace = watch_namespace();
    let context = Arc::new(ContextData {
        client: client.clone(),
        http: reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(1))
            .timeout(Duration::from_secs(3))
            .redirect(reqwest::redirect::Policy::none())
            .build()?,
    });
    let clusters = Api::<RustQueue>::namespaced(client.clone(), &namespace);
    let stateful_sets = Api::<StatefulSet>::namespaced(client, &namespace);
    tracing::info!(%namespace, "share-nothing RustQueue Operator started");
    Controller::new(clusters, watcher::Config::default())
        .owns(stateful_sets, watcher::Config::default())
        .run(reconcile, error_policy, context)
        .for_each(|result| async move {
            match result {
                Ok((reference, _)) => tracing::debug!(object = %reference.name, "reconciled"),
                Err(error) => tracing::warn!(%error, "controller stream error"),
            }
        })
        .await;
    Ok(())
}

async fn reconcile(
    cluster: Arc<RustQueue>,
    context: Arc<ContextData>,
) -> Result<Action, ReconcileError> {
    Ok(reconcile_inner(cluster, context).await?)
}

async fn reconcile_inner(
    cluster: Arc<RustQueue>,
    context: Arc<ContextData>,
) -> anyhow::Result<Action> {
    validate(&cluster)?;
    let namespace = cluster
        .namespace()
        .context("RustQueue must be namespaced")?;
    let auth = auth::ensure(&context.client, &cluster, &namespace).await?;
    let eligible = nodes::eligible(&context.client, &cluster.spec.eligible_node_selector).await?;
    let desired = (eligible as i32).min(cluster.spec.max_brokers);
    let statefulsets: Api<StatefulSet> = Api::namespaced(context.client.clone(), &namespace);
    let current = statefulsets
        .get_opt(&cluster.name_any())
        .await?
        .and_then(|set| set.spec.and_then(|spec| spec.replicas))
        .unwrap_or(0);
    let target_preflight = preflight::target_image(&context, &cluster, &namespace).await?;
    if let Some(action) =
        preflight_action(&context, &cluster, desired, current, target_preflight).await?
    {
        return Ok(action);
    }
    let mut active_feature_level = cluster.spec.storage_feature_level;
    if current > 0 {
        let broker_preflight =
            preflight::current_brokers(&context, &cluster, &namespace, &auth).await?;
        match broker_preflight {
            preflight::Outcome::Ready {
                active_feature_level: active,
            } => active_feature_level = active,
            outcome => {
                if let Some(action) =
                    preflight_action(&context, &cluster, desired, current, outcome).await?
                {
                    return Ok(action);
                }
            }
        }
    }
    preflight::cleanup_old_probes(&context, &cluster, &namespace).await?;
    let applied_replicas = if current == 0 || desired > current {
        desired
    } else {
        current
    };
    let set = resources::build(BuildInput {
        cluster: &cluster,
        replicas: applied_replicas,
        secret_name: &auth.name,
        secret_revision: &auth.revision,
    })?;
    let revision = set.revision.clone();
    apply::resources(&context.client, &namespace, set).await?;

    let (phase, message) = if desired < cluster.spec.min_brokers {
        (
            "InsufficientNodes".into(),
            format!(
                "{eligible} eligible nodes; minimum is {}",
                cluster.spec.min_brokers
            ),
        )
    } else if current > desired {
        (
            "Draining".into(),
            drain::scale_down_one(&context, &cluster, &namespace, current, &auth).await?,
        )
    } else if current == desired && desired > 0 {
        match drain::rollout_one(&context, &cluster, &namespace, current, &revision, &auth).await? {
            Some(message) => ("Rolling".into(), message),
            None => {
                drain::resume_current(&context, &cluster, &namespace, &revision, &auth).await?;
                ("Ready".into(), format!("{desired} broker shards desired"))
            }
        }
    } else {
        (
            "Scaling".into(),
            format!("scaling from {current} to {desired}"),
        )
    };
    let ready = nodes::ready_brokers(&context.client, &namespace, &cluster.name_any()).await?;
    apply::status(
        &context.client,
        &cluster,
        RustQueueStatus {
            observed_generation: cluster.metadata.generation,
            desired_brokers: desired,
            ready_brokers: ready,
            phase,
            message,
            active_storage_feature_level: active_feature_level,
        },
    )
    .await?;
    Ok(Action::requeue(Duration::from_secs(5)))
}

async fn preflight_action(
    context: &ContextData,
    cluster: &RustQueue,
    desired: i32,
    current: i32,
    outcome: preflight::Outcome,
) -> anyhow::Result<Option<Action>> {
    let (phase, message) = match outcome {
        preflight::Outcome::Ready { .. } => return Ok(None),
        preflight::Outcome::Pending(message) => ("Preflight".to_owned(), message),
        preflight::Outcome::Blocked(message) => ("PreflightBlocked".to_owned(), message),
    };
    let ready = nodes::ready_brokers(
        &context.client,
        &cluster.namespace().expect("validated namespace"),
        &cluster.name_any(),
    )
    .await?;
    apply::status(
        &context.client,
        cluster,
        RustQueueStatus {
            observed_generation: cluster.metadata.generation,
            desired_brokers: desired,
            ready_brokers: ready.min(current),
            phase,
            message,
            active_storage_feature_level: cluster
                .status
                .as_ref()
                .map_or(1, |status| status.active_storage_feature_level.max(1)),
        },
    )
    .await?;
    Ok(Some(Action::requeue(Duration::from_secs(5))))
}

fn validate(cluster: &RustQueue) -> anyhow::Result<()> {
    if cluster.spec.image.trim().is_empty() {
        bail!("spec.image is required");
    }
    if cluster.spec.min_brokers < 1 || cluster.spec.max_brokers < cluster.spec.min_brokers {
        bail!("broker limits must satisfy 1 <= minBrokers <= maxBrokers");
    }
    if cluster.spec.storage_class_name.trim().is_empty()
        || cluster.spec.storage_size.trim().is_empty()
    {
        bail!("storageClassName and storageSize are required");
    }
    if cluster.spec.storage_feature_level == 0 {
        bail!("storageFeatureLevel must be greater than zero");
    }
    if cluster.spec.disk_low_watermark_percent >= cluster.spec.disk_high_watermark_percent
        || cluster.spec.disk_high_watermark_percent > 100
    {
        bail!("disk watermarks must satisfy low < high <= 100");
    }
    if cluster.spec.bootstrap_retention_seconds == 0
        || cluster.spec.max_message_bytes == 0
        || cluster.spec.max_message_bytes > 32 * 1024 * 1024
        || cluster.spec.max_backlog_messages == 0
    {
        bail!("queue limits are outside the stable v7 contract");
    }
    Ok(())
}

fn error_policy(
    cluster: Arc<RustQueue>,
    error: &ReconcileError,
    _context: Arc<ContextData>,
) -> Action {
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
