use super::status::{self, OperationUpdate, StatusBuilder};
use super::{auth, drain, ContextData};
use crate::RustQueue;
use kube::ResourceExt;

pub(super) struct PendingOperation {
    id: String,
    kind: String,
    phase: String,
    target: String,
    revision: String,
    message: String,
    previous_image: Option<String>,
    current_broker: Option<String>,
}

impl PendingOperation {
    pub fn apply<'a>(&self, builder: StatusBuilder<'a>) -> StatusBuilder<'a> {
        builder.operation(OperationUpdate {
            id: &self.id,
            kind: &self.kind,
            phase: &self.phase,
            target: &self.target,
            revision: &self.revision,
            message: &self.message,
            previous_image: self.previous_image.clone(),
            current_broker: self.current_broker.clone(),
        })
    }

    pub fn audit(&self, cluster: &RustQueue) {
        let unchanged = cluster
            .status
            .as_ref()
            .and_then(|status| status.current_operation.as_ref())
            .is_some_and(|operation| operation.id == self.id && operation.phase == self.phase);
        if !unchanged {
            tracing::info!(
                cluster = %cluster.name_any(),
                operation_id = %self.id,
                operation_kind = %self.kind,
                operation_phase = %self.phase,
                target = %self.target,
                current_broker = ?self.current_broker,
                "RustQueue operation transitioned"
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn reconcile(
    context: &ContextData,
    cluster: &RustQueue,
    namespace: &str,
    auth: &auth::AuthSecret,
    eligible: usize,
    desired: i32,
    current: i32,
    revision: &str,
    effective_image: &str,
    disruptions_allowed: bool,
) -> anyhow::Result<(String, String, Option<PendingOperation>)> {
    if insufficient_eligible_nodes(eligible, desired, cluster.spec.min_brokers) {
        let resumed = drain::resume_all(context, cluster, namespace, auth).await?;
        let resumed = if resumed.is_empty() {
            String::new()
        } else {
            format!("; resumed disrupted Brokers {}", resumed.join(", "))
        };
        return Ok((
            "InsufficientNodes".into(),
            format!(
                "{eligible} eligible nodes; desired is {desired} and minimum is {}{resumed}",
                cluster.spec.min_brokers
            ),
            None,
        ));
    }
    if let Some(request) = &cluster.spec.maintenance {
        if request.enabled && !disruptions_allowed {
            drain::resume_all(context, cluster, namespace, auth).await?;
            return Ok(waiting_for_kodo_cutover());
        }
        if request.enabled {
            return reconcile_maintenance(context, cluster, namespace, auth, revision, request)
                .await;
        }
        // A completed maintenance request may remain in spec. Resume its
        // target, then continue with scale and rollout reconciliation.
        drain::resume_one(context, namespace, &request.broker, auth).await?;
    }
    if current > desired {
        if !disruptions_allowed {
            drain::resume_all(context, cluster, namespace, auth).await?;
            return Ok(waiting_for_kodo_cutover());
        }
        return reconcile_scale_down(
            context, cluster, namespace, auth, current, desired, revision,
        )
        .await;
    }
    if current < desired {
        let message = format!("scaling from {current} to {desired}");
        let target = desired.to_string();
        let operation = PendingOperation {
            id: status::operation_id("scale-up", &target, revision),
            kind: "ScaleUp".into(),
            phase: "WaitingForReady".into(),
            target,
            revision: revision.into(),
            message: message.clone(),
            previous_image: None,
            current_broker: None,
        };
        return Ok(("Scaling".into(), message, Some(operation)));
    }
    if desired == 0 {
        return Ok(("Ready".into(), "no eligible Broker nodes".into(), None));
    }
    if !disruptions_allowed {
        drain::resume_all(context, cluster, namespace, auth).await?;
        if drain::brokers_current_and_ready(context, cluster, namespace, revision, current).await? {
            return Ok((
                "Ready".into(),
                "all Brokers already match the target revision".into(),
                None,
            ));
        }
        return Ok(waiting_for_kodo_cutover());
    }
    reconcile_rollout(
        context,
        cluster,
        namespace,
        auth,
        current,
        revision,
        effective_image,
    )
    .await
}

fn insufficient_eligible_nodes(eligible: usize, desired: i32, minimum: i32) -> bool {
    desired < minimum
        || usize::try_from(desired)
            .map(|desired| eligible < desired)
            .unwrap_or(true)
}

fn waiting_for_kodo_cutover() -> (String, String, Option<PendingOperation>) {
    (
        "WaitingForKodoCutover".into(),
        "waiting for Kodo Gateway cutover safety gates before disrupting a Broker".into(),
        None,
    )
}

async fn reconcile_maintenance(
    context: &ContextData,
    cluster: &RustQueue,
    namespace: &str,
    auth: &auth::AuthSecret,
    revision: &str,
    request: &crate::crd::BrokerMaintenance,
) -> anyhow::Result<(String, String, Option<PendingOperation>)> {
    let progress = drain::maintenance(
        context,
        cluster,
        namespace,
        &request.broker,
        request.enabled,
        auth,
    )
    .await?;
    let target = progress.target.unwrap_or_else(|| request.broker.clone());
    let id = status::operation_id(
        "maintenance",
        &target,
        if request.enabled {
            "enabled"
        } else {
            "disabled"
        },
    );
    let operation = PendingOperation {
        id,
        kind: "Maintenance".into(),
        phase: progress.phase.into(),
        target,
        revision: revision.into(),
        message: progress.message.clone(),
        previous_image: None,
        current_broker: Some(request.broker.clone()),
    };
    let phase = if progress.phase == "Blocked" {
        "MaintenanceBlocked"
    } else if request.enabled {
        "Maintenance"
    } else {
        "Ready"
    };
    Ok((phase.into(), progress.message, Some(operation)))
}

async fn reconcile_scale_down(
    context: &ContextData,
    cluster: &RustQueue,
    namespace: &str,
    auth: &auth::AuthSecret,
    current: i32,
    desired: i32,
    revision: &str,
) -> anyhow::Result<(String, String, Option<PendingOperation>)> {
    let progress = drain::scale_down_one(context, cluster, namespace, current, auth).await?;
    let target = progress
        .target
        .clone()
        .unwrap_or_else(|| format!("{}-{}", cluster.name_any(), current - 1));
    let operation = PendingOperation {
        id: status::operation_id("scale-down", &target, &desired.to_string()),
        kind: "ScaleDown".into(),
        phase: progress.phase.into(),
        target,
        revision: revision.into(),
        message: progress.message.clone(),
        previous_image: None,
        current_broker: progress.target,
    };
    Ok(("Draining".into(), progress.message, Some(operation)))
}

async fn reconcile_rollout(
    context: &ContextData,
    cluster: &RustQueue,
    namespace: &str,
    auth: &auth::AuthSecret,
    current: i32,
    revision: &str,
    effective_image: &str,
) -> anyhow::Result<(String, String, Option<PendingOperation>)> {
    let kind = if cluster.spec.rollout.rollback_to_image.is_some() {
        "Rollback"
    } else {
        "Rollout"
    };
    let operation_revision = format!("{revision}:{}", cluster.spec.rollout.retry_nonce);
    let id = status::operation_id(
        &kind.to_ascii_lowercase(),
        effective_image,
        &operation_revision,
    );
    let previous = cluster
        .status
        .as_ref()
        .and_then(|status| status.current_operation.as_ref())
        .filter(|operation| operation.id == id);
    let timed_out = previous
        .filter(|operation| rollout_phase_can_timeout(&operation.phase))
        .and_then(|operation| status::elapsed_seconds(&operation.started_at))
        .is_some_and(|elapsed| elapsed >= cluster.spec.rollout.timeout_seconds);
    let previous_image = match previous.and_then(|operation| operation.previous_image.clone()) {
        Some(image) => Some(image),
        None => drain::previous_image(context, cluster, namespace, revision).await?,
    };
    if timed_out || previous.is_some_and(|operation| operation.phase == "Failed") {
        if let Some(target) = previous.and_then(|operation| operation.current_broker.as_deref()) {
            drain::resume_one(context, namespace, target, auth).await?;
        }
    }
    let progress = if let Some(failed) = previous.filter(|operation| operation.phase == "Failed") {
        drain::Progress {
            target: failed.current_broker.clone(),
            phase: "Failed",
            message: failed.message.clone(),
        }
    } else if timed_out {
        drain::Progress {
            target: previous.and_then(|operation| operation.current_broker.clone()),
            phase: "Failed",
            message: format!(
                "rollout exceeded its {} second timeout; change retryNonce or request rollback",
                cluster.spec.rollout.timeout_seconds
            ),
        }
    } else {
        drain::rollout_one(
            context,
            cluster,
            namespace,
            drain::RolloutOptions {
                replicas: current,
                desired_revision: revision,
                paused: cluster.spec.rollout.paused,
                require_canary_approval: cluster.spec.rollout.require_canary_approval,
                approved_revision: cluster.spec.rollout.approved_revision.as_deref(),
            },
            auth,
        )
        .await?
    };
    if progress.phase == "Completed" {
        drain::resume_current(context, cluster, namespace, revision, auth).await?;
    }
    let phase = match progress.phase {
        "Completed" => "Ready",
        "Blocked" => "RolloutBlocked",
        "Failed" => "RolloutFailed",
        "Paused" => "RolloutPaused",
        "AwaitingCanaryApproval" => "RolloutAwaitingApproval",
        _ => "Rolling",
    };
    let operation = PendingOperation {
        id,
        kind: kind.into(),
        phase: progress.phase.into(),
        target: effective_image.into(),
        revision: revision.into(),
        message: progress.message.clone(),
        previous_image,
        current_broker: progress.target,
    };
    Ok((phase.into(), progress.message, Some(operation)))
}

fn rollout_phase_can_timeout(phase: &str) -> bool {
    !matches!(
        phase,
        "Completed" | "Failed" | "Blocked" | "Paused" | "AwaitingCanaryApproval"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_three_broker_profile_reports_insufficient_eligible_nodes() {
        assert!(insufficient_eligible_nodes(2, 3, 3));
        assert!(!insufficient_eligible_nodes(3, 3, 3));
        assert!(insufficient_eligible_nodes(0, 0, 1));
    }

    #[test]
    fn terminal_and_human_gated_rollouts_never_time_out() {
        for phase in [
            "Completed",
            "Failed",
            "Blocked",
            "Paused",
            "AwaitingCanaryApproval",
        ] {
            assert!(!rollout_phase_can_timeout(phase), "{phase}");
        }
        for phase in ["Draining", "Replacing", "WaitingForReady"] {
            assert!(rollout_phase_can_timeout(phase), "{phase}");
        }
    }
}
