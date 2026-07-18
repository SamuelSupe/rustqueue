mod apply;
mod auth;
mod drain;
mod leadership;
mod nodes;
mod operations;
mod preflight;
mod status;
mod storage;

use crate::resources::{self, BuildInput};
use crate::RustQueue;
use anyhow::{bail, Context as _};
use futures::StreamExt;
use k8s_openapi::api::apps::v1::StatefulSet;
use kube::api::Api;
use kube::runtime::controller::{Action, Controller};
use kube::runtime::watcher;
use kube::{Client, ResourceExt};
use status::{OperationUpdate, StatusBuilder};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub(super) struct ContextData {
    pub client: Client,
    pub http: reqwest::Client,
    pub leader: Arc<AtomicBool>,
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct ReconcileError(#[from] anyhow::Error);

pub async fn run(leader: Arc<AtomicBool>) -> anyhow::Result<()> {
    let client = Client::try_default().await?;
    let namespace = watch_namespace();
    leadership::start(client.clone(), namespace.clone(), Arc::clone(&leader));
    let context = Arc::new(ContextData {
        client: client.clone(),
        http: reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(1))
            .timeout(Duration::from_secs(3))
            .redirect(reqwest::redirect::Policy::none())
            .build()?,
        leader,
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
    match reconcile_inner(Arc::clone(&cluster), Arc::clone(&context)).await {
        Ok(action) => Ok(action),
        Err(error) => {
            if context.leader.load(Ordering::Acquire) {
                record_reconcile_error(&context, &cluster, &error).await;
            }
            Err(error.into())
        }
    }
}

async fn reconcile_inner(
    cluster: Arc<RustQueue>,
    context: Arc<ContextData>,
) -> anyhow::Result<Action> {
    if !context.leader.load(Ordering::Acquire) {
        return Ok(Action::requeue(Duration::from_secs(2)));
    }
    validate(&cluster)?;
    let namespace = cluster
        .namespace()
        .context("RustQueue must be namespaced")?;
    let effective_image = cluster
        .spec
        .rollout
        .rollback_to_image
        .as_deref()
        .unwrap_or(&cluster.spec.image);
    let auth = auth::ensure(&context.client, &cluster, &namespace).await?;
    let eligible = nodes::eligible(&context.client, &cluster.spec.eligible_node_selector).await?;
    let desired = (eligible as i32).min(cluster.spec.max_brokers);
    let statefulsets: Api<StatefulSet> = Api::namespaced(context.client.clone(), &namespace);
    let current_set = statefulsets.get_opt(&cluster.name_any()).await?;
    let current = current_set
        .as_ref()
        .and_then(|set| set.spec.as_ref())
        .and_then(|spec| spec.replicas)
        .unwrap_or(0);

    let target_preflight =
        preflight::target_image(&context, &cluster, &namespace, effective_image).await?;
    if preflight_status(
        &context,
        &cluster,
        desired,
        current,
        effective_image,
        &target_preflight,
    )
    .await?
    {
        return Ok(Action::requeue(Duration::from_secs(5)));
    }
    let mut active_feature_level = previous_feature_level(&cluster);
    if current > 0 {
        let broker_preflight =
            preflight::current_brokers(&context, &cluster, &namespace, &auth).await?;
        if let preflight::Outcome::Ready {
            active_feature_level: active,
        } = &broker_preflight
        {
            active_feature_level = *active;
        } else if preflight_status(
            &context,
            &cluster,
            desired,
            current,
            effective_image,
            &broker_preflight,
        )
        .await?
        {
            return Ok(Action::requeue(Duration::from_secs(5)));
        }
    }
    preflight::cleanup_old_probes(&context, &cluster, &namespace, effective_image).await?;

    let storage = storage::reconcile(&context.client, &cluster, &namespace, desired).await?;
    if storage.state != storage::StorageState::Ready {
        let ready = nodes::ready_brokers(&context.client, &namespace, &cluster.name_any()).await?;
        let blocked = storage.state == storage::StorageState::Blocked;
        let phase = if blocked {
            "StorageBlocked"
        } else {
            "StorageResizing"
        };
        let status =
            StatusBuilder::new(&cluster, desired, ready.min(current), active_feature_level)
                .summary(phase, &storage.message)
                .condition("Ready", false, phase, &storage.message)
                .condition("Progressing", !blocked, phase, &storage.message)
                .condition("Degraded", blocked, phase, &storage.message)
                .condition("StorageReady", false, phase, &storage.message)
                .condition(
                    "Upgradeable",
                    true,
                    "PreflightPassed",
                    "binary compatibility checks passed",
                )
                .orphaned_pvcs(storage.orphaned_pvcs)
                .build();
        apply::status(&context.client, &cluster, status).await?;
        return Ok(Action::requeue(Duration::from_secs(5)));
    }

    let applied_replicas = if current == 0 || desired > current {
        desired
    } else {
        current
    };
    let mounted_secret_revision =
        auth::mounted_secret_revision(&context.client, &cluster, &namespace, &auth).await?;
    let claim_template_size =
        storage::claim_template_size(current_set.as_ref(), &cluster.spec.storage_size);
    let set = resources::build(BuildInput {
        cluster: &cluster,
        replicas: applied_replicas,
        image: effective_image,
        claim_template_size: &claim_template_size,
        secret_name: &auth.name,
        mounted_secret_revision: &mounted_secret_revision,
    })?;
    let revision = set.revision.clone();
    apply::resources(&context.client, &namespace, set).await?;

    let (phase, message, operation) = operations::reconcile(
        &context,
        &cluster,
        &namespace,
        &auth,
        eligible,
        desired,
        current,
        &revision,
        effective_image,
    )
    .await?;
    let broker_health =
        nodes::broker_health(&context.client, &namespace, &cluster.name_any()).await?;
    let ready = broker_health.ready;
    let ready_condition = phase == "Ready" && ready == desired;
    let degraded = matches!(
        phase.as_str(),
        "InsufficientNodes" | "RolloutBlocked" | "RolloutFailed"
    );
    let progressing = !ready_condition && !degraded && phase != "Maintenance";
    let maintenance_enabled = cluster
        .spec
        .maintenance
        .as_ref()
        .is_some_and(|request| request.enabled);
    let mut builder = StatusBuilder::new(&cluster, desired, ready, active_feature_level)
        .summary(&phase, &message)
        .condition("Ready", ready_condition, &phase, &message)
        .condition("Progressing", progressing, &phase, &message)
        .condition("Degraded", degraded, &phase, &message)
        .condition("StorageReady", true, "CapacityReady", &storage.message)
        .condition(
            "Upgradeable",
            true,
            "PreflightPassed",
            "binary compatibility checks passed",
        )
        .condition(
            "Maintenance",
            maintenance_enabled,
            if maintenance_enabled {
                "Requested"
            } else {
                "NotRequested"
            },
            if maintenance_enabled {
                "a Broker is intentionally drained"
            } else {
                "no Broker maintenance is requested"
            },
        )
        .condition(
            "OrphanedPVCs",
            !storage.orphaned_pvcs.is_empty(),
            if storage.orphaned_pvcs.is_empty() {
                "None"
            } else {
                "RetainedAfterScaleDown"
            },
            if storage.orphaned_pvcs.is_empty() {
                "no retained orphan PVCs"
            } else {
                "retained PVCs require an explicit operator decision"
            },
        )
        .condition(
            "BrokersAvailable",
            broker_health.unavailable.is_empty(),
            if broker_health.unavailable.is_empty() {
                "AllReady"
            } else {
                "PodsUnavailable"
            },
            if broker_health.unavailable.is_empty() {
                "all Broker Pods are Ready".into()
            } else {
                broker_health.unavailable.join("; ")
            },
        )
        .orphaned_pvcs(storage.orphaned_pvcs);
    if let Some(operation) = &operation {
        operation.audit(&cluster);
        builder = operation.apply(builder);
    }
    apply::status(&context.client, &cluster, builder.build()).await?;
    Ok(Action::requeue(Duration::from_secs(5)))
}

async fn preflight_status(
    context: &ContextData,
    cluster: &RustQueue,
    desired: i32,
    current: i32,
    target_image: &str,
    outcome: &preflight::Outcome,
) -> anyhow::Result<bool> {
    let (phase, message, blocked) = match outcome {
        preflight::Outcome::Ready { .. } => return Ok(false),
        preflight::Outcome::Pending(message) => ("Preflight", message, false),
        preflight::Outcome::Blocked(message) => ("PreflightBlocked", message, true),
    };
    let ready = nodes::ready_brokers(
        &context.client,
        &cluster.namespace().expect("validated namespace"),
        &cluster.name_any(),
    )
    .await?;
    let kind = if cluster.spec.rollout.rollback_to_image.is_some() {
        "Rollback"
    } else {
        "Rollout"
    };
    let preflight_revision = format!(
        "preflight:{}:{}",
        cluster.spec.storage_feature_level, cluster.spec.rollout.retry_nonce
    );
    let existing = cluster
        .status
        .as_ref()
        .and_then(|status| status.current_operation.as_ref())
        .filter(|operation| operation.kind == kind && operation.target == target_image);
    let revision = existing
        .map(|operation| operation.revision.clone())
        .unwrap_or(preflight_revision);
    let operation_id = existing
        .map(|operation| operation.id.clone())
        .unwrap_or_else(|| {
            status::operation_id(&kind.to_ascii_lowercase(), target_image, &revision)
        });
    let status = StatusBuilder::new(
        cluster,
        desired,
        ready.min(current),
        previous_feature_level(cluster),
    )
    .summary(phase, message)
    .condition("Ready", false, phase, message)
    .condition("Progressing", !blocked, phase, message)
    .condition("Degraded", blocked, phase, message)
    .condition("Upgradeable", false, phase, message)
    .operation(OperationUpdate {
        id: &operation_id,
        kind,
        phase: if blocked { "Blocked" } else { "Preflight" },
        target: target_image,
        revision: &revision,
        message,
        previous_image: existing
            .and_then(|operation| operation.previous_image.clone())
            .or_else(|| {
                cluster
                    .spec
                    .rollout
                    .rollback_to_image
                    .as_ref()
                    .map(|_| cluster.spec.image.clone())
            }),
        current_broker: None,
    })
    .build();
    apply::status(&context.client, cluster, status).await?;
    Ok(true)
}

fn previous_feature_level(cluster: &RustQueue) -> u32 {
    cluster
        .status
        .as_ref()
        .map_or(1, |status| status.active_storage_feature_level.max(1))
}

async fn record_reconcile_error(context: &ContextData, cluster: &RustQueue, error: &anyhow::Error) {
    let previous = cluster.status.as_ref();
    let desired = previous.map_or(cluster.spec.min_brokers, |status| status.desired_brokers);
    let ready = previous.map_or(0, |status| status.ready_brokers);
    let message = format!("reconciliation failed: {error:#}");
    let status = StatusBuilder::new(cluster, desired, ready, previous_feature_level(cluster))
        .summary("ReconcileError", &message)
        .condition("Ready", false, "ReconcileError", &message)
        .condition("Progressing", false, "ReconcileError", &message)
        .condition("Degraded", true, "ReconcileError", &message)
        .build();
    if let Err(status_error) = apply::status(&context.client, cluster, status).await {
        tracing::warn!(%status_error, "failed to persist reconciliation error status");
    }
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
        || cluster.spec.message_index_cache_bytes == 0
        || cluster.spec.max_topics == 0
        || cluster.spec.max_publish_workers == 0
        || cluster.spec.publish_worker_idle_seconds == 0
        || cluster.spec.max_detailed_metric_series == 0
    {
        bail!("queue limits are outside the stable v7 contract");
    }
    if cluster.spec.rollout.timeout_seconds == 0 || cluster.spec.rollout.timeout_seconds > 86_400 {
        bail!("rollout timeoutSeconds must be between 1 and 86400");
    }
    if cluster
        .spec
        .rollout
        .rollback_to_image
        .as_ref()
        .is_some_and(|image| image.trim().is_empty())
    {
        bail!("rollout rollbackToImage cannot be empty");
    }
    if cluster
        .spec
        .broker_scheduling
        .topology_key
        .trim()
        .is_empty()
        || cluster.spec.broker_resources.cpu_request.trim().is_empty()
        || cluster
            .spec
            .broker_resources
            .memory_request
            .trim()
            .is_empty()
    {
        bail!("broker scheduling and resource requests cannot be empty");
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
