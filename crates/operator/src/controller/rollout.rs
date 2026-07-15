use super::Context;
use crate::crd::{RustQueueCluster, UpgradeStatus};
use crate::layout::{BrokerPlan, ClusterLayout};
use crate::upgrade::{self, PodRolloutState, RolloutTarget};
use anyhow::Context as _;
use serde_json::json;
use std::sync::Arc;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

pub enum Decision {
    Settled(Option<UpgradeStatus>),
    Delete {
        status: UpgradeStatus,
        broker: BrokerPlan,
        pod_name: String,
        needs_maintenance: bool,
    },
    Release {
        status: UpgradeStatus,
        broker: BrokerPlan,
    },
}

pub fn decide(
    cluster: &RustQueueCluster,
    layout: &ClusterLayout,
    pods: &[PodRolloutState],
    target: &RolloutTarget<'_>,
) -> Decision {
    if upgrade::complete(pods, target) {
        return Decision::Settled(None);
    }
    let old = cluster
        .status
        .as_ref()
        .and_then(|status| status.upgrade.clone());
    let changed_target = old
        .as_ref()
        .is_some_and(|status| status.target_image != target.image);
    let mut status = old.unwrap_or_else(|| new_status(cluster, target.image));
    status.target_image = target.image.into();
    if changed_target || status.observed_retry_generation != cluster.spec.upgrade.retry_generation {
        status.paused = false;
        status.reason.clear();
        status.observed_retry_generation = cluster.spec.upgrade.retry_generation;
    }

    if let Some(node_id) = status.current_node_id {
        let Some(broker) = layout.broker(node_id).cloned() else {
            status.paused = true;
            status.reason = "current Broker is absent from the desired layout".into();
            return Decision::Settled(Some(status));
        };
        if let Some(pod) = pods.iter().find(|pod| pod.node_id == node_id) {
            if pod_matches(pod, target) && pod.ready {
                return Decision::Release { status, broker };
            }
            if !pod_matches(pod, target) {
                return Decision::Delete {
                    status,
                    broker,
                    pod_name: pod.pod_name.clone(),
                    needs_maintenance: pod.ready,
                };
            }
        }
        if deadline_expired(&status, cluster.spec.upgrade.progress_deadline_seconds) {
            status.paused = true;
            status.reason = format!(
                "Broker {node_id} did not become Ready before the rollout deadline; bump spec.upgrade.retryGeneration to resume"
            );
        }
        return Decision::Settled(Some(status));
    }

    if !cluster.spec.upgrade.automatic {
        status.paused = true;
        status.reason = "automatic rollout is disabled".into();
        return Decision::Settled(Some(status));
    }
    if status.paused {
        return Decision::Settled(Some(status));
    }
    let Some(candidate) =
        upgrade::next_candidate(pods, target, cluster.spec.upgrade.max_unavailable_per_cell)
    else {
        status.reason = "waiting for every Cell to regain its disruption budget".into();
        return Decision::Settled(Some(status));
    };
    let Some(broker) = layout.broker(candidate.node_id).cloned() else {
        status.paused = true;
        status.reason = "rollout candidate is absent from desired layout".into();
        return Decision::Settled(Some(status));
    };
    status.current_node_id = Some(candidate.node_id);
    status.started_at = Some(crate::status::now());
    status.reason = "transferring leadership before Pod replacement".into();
    Decision::Delete {
        status,
        broker,
        pod_name: candidate.pod_name.clone(),
        needs_maintenance: candidate.ready,
    }
}

pub async fn prepare_delete(
    context: &Arc<Context>,
    broker: &BrokerPlan,
    admin_token: &str,
) -> anyhow::Result<()> {
    set_maintenance(context, broker, admin_token, true).await
}

pub async fn release(
    context: &Arc<Context>,
    cluster: &RustQueueCluster,
    broker: &BrokerPlan,
    admin_token: &str,
) -> anyhow::Result<UpgradeStatus> {
    let info = context
        .http
        .get(format!("http://{}:4151/info", pod_dns(context, broker)))
        .send()
        .await
        .context("query replacement Broker info")?
        .error_for_status()
        .context("replacement Broker info rejected")?
        .json::<serde_json::Value>()
        .await
        .context("decode replacement Broker info")?;
    anyhow::ensure!(
        info["version"].is_string(),
        "replacement Broker omitted version"
    );
    set_maintenance(context, broker, admin_token, false).await?;
    Ok(UpgradeStatus {
        target_image: cluster.spec.image.clone(),
        current_node_id: None,
        started_at: None,
        paused: false,
        reason: "replacement is compatible and Ready".into(),
        observed_retry_generation: cluster.spec.upgrade.retry_generation,
    })
}

async fn set_maintenance(
    context: &Arc<Context>,
    broker: &BrokerPlan,
    admin_token: &str,
    enabled: bool,
) -> anyhow::Result<()> {
    context
        .http
        .post(format!(
            "http://{}:4151/v1/cluster/nodes/{}/maintenance",
            pod_dns(context, broker),
            broker.node_id
        ))
        .bearer_auth(admin_token)
        .json(&json!({
            "enabled": enabled,
            "ttl_seconds": if enabled { Some(1800_u64) } else { None },
            "reason": if enabled { "kubernetes rolling replacement" } else { "" }
        }))
        .send()
        .await
        .context("set Broker maintenance lease")?
        .error_for_status()
        .context("Broker rejected maintenance lease")?;
    Ok(())
}

fn pod_dns(context: &Context, broker: &BrokerPlan) -> String {
    format!(
        "{}.{}.{}.svc",
        broker.pod_name, broker.headless_service, context.namespace
    )
}

fn new_status(cluster: &RustQueueCluster, image: &str) -> UpgradeStatus {
    UpgradeStatus {
        target_image: image.into(),
        current_node_id: None,
        started_at: None,
        paused: false,
        reason: "rollout pending".into(),
        observed_retry_generation: cluster.spec.upgrade.retry_generation,
    }
}

fn pod_matches(pod: &PodRolloutState, target: &RolloutTarget<'_>) -> bool {
    pod.image == target.image
        && target.tls_revisions.get(&pod.node_id) == Some(&pod.tls_revision)
        && target.config_revisions.get(&pod.node_id) == Some(&pod.config_revision)
        && target.target_nodes.get(&pod.node_id) == Some(&pod.target_node)
        && pod.rollout_revision == target.rollout_revision
}

fn deadline_expired(status: &UpgradeStatus, deadline_seconds: u64) -> bool {
    status
        .started_at
        .as_ref()
        .and_then(|value| OffsetDateTime::parse(value, &Rfc3339).ok())
        .is_some_and(|started| {
            OffsetDateTime::now_utc().unix_timestamp() - started.unix_timestamp()
                > deadline_seconds as i64
        })
}
