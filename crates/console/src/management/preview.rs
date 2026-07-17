use super::security::authorize_mutation;
use super::{ensure_enabled, ManagementError};
use crate::app::AppState;
use crate::model::{BrokerView, Snapshot};
use crate::resources;
use crate::session::{now_ms, ActionChallenge};
use axum::extract::State;
use axum::http::HeaderMap;
use axum::Json;
use kube::api::Api;
use kube::ResourceExt;
use rustqueue_operator::RustQueue;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Deserialize)]
pub struct PreviewRequest {
    pub kind: String,
    pub action: String,
    pub topic: String,
    pub channel: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Impact {
    pub owners: Vec<String>,
    pub stored_messages: u64,
    pub depth: u64,
    pub in_flight: u64,
    pub deferred: u64,
    pub connections: i64,
    pub warnings: Vec<String>,
}

pub async fn preview(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<PreviewRequest>,
) -> Result<Json<serde_json::Value>, ManagementError> {
    ensure_enabled(&state)?;
    let (session_id, _) = authorize_mutation(&state, &headers)?;
    validate_request(&request)?;
    let snapshot = current_snapshot(&state)?;
    let target = resolve_target(&snapshot, &request)?;
    ensure_owners_healthy(&snapshot, &target.owners)?;
    let subject = subject(&state, &request).await?;
    let confirmation = requires_confirmation(&request.action).then(|| {
        request
            .channel
            .clone()
            .unwrap_or_else(|| request.topic.clone())
    });
    let challenge = state
        .sessions
        .issue_challenge(
            &session_id,
            ActionChallenge {
                token: String::new(),
                kind: request.kind.clone(),
                action: request.action.clone(),
                topic: request.topic.clone(),
                channel: request.channel.clone(),
                subject_uid: subject.uid,
                resource_version: subject.resource_version,
                subject_kind: subject.kind,
                owners: target.owners.clone(),
                confirmation: confirmation.clone(),
                expires_at_ms: now_ms() + 60_000,
            },
        )
        .ok_or_else(|| ManagementError::unauthorized("management session expired"))?;
    Ok(Json(json!({
        "action_token": challenge.token,
        "expires_at_ms": challenge.expires_at_ms,
        "confirmation_required": confirmation,
        "impact": target.impact,
    })))
}

pub fn current_snapshot(state: &AppState) -> Result<Snapshot, ManagementError> {
    let snapshot = state
        .live
        .snapshot()
        .ok_or_else(|| ManagementError::unavailable("cluster snapshot is not ready"))?;
    let maximum_age = state
        .config
        .poll_interval
        .as_millis()
        .saturating_mul(3)
        .saturating_add(2_000) as u64;
    if now_ms().saturating_sub(snapshot.collected_at_ms) > maximum_age {
        return Err(ManagementError::unavailable("cluster snapshot is stale"));
    }
    if !snapshot.complete
        || !snapshot.management.enabled
        || !snapshot.management.crd_fresh
        || !snapshot.management.registry_available
    {
        return Err(ManagementError::unavailable(
            "cluster, Registry, CRD catalog, or broker observation is incomplete",
        ));
    }
    Ok(snapshot)
}

pub fn ensure_owners_healthy(
    snapshot: &Snapshot,
    owners: &[String],
) -> Result<(), ManagementError> {
    if owners.is_empty() {
        return Err(ManagementError::unavailable(
            "management target has no eligible owner",
        ));
    }
    for owner in owners {
        let broker = snapshot
            .brokers
            .iter()
            .find(|broker| broker.name == *owner)
            .ok_or_else(|| ManagementError::unavailable(format!("owner {owner} is absent")))?;
        let healthy = broker.ready
            && broker.error.is_none()
            && broker.observation.as_ref().is_some_and(|observation| {
                observation.registry_revision > 0
                    && observation.readiness.process_ready
                    && observation.readiness.storage_healthy
                    && observation.readiness.disk_ready
                    && observation.readiness.management_fences_ready
                    && !observation.readiness.draining
            });
        if !healthy {
            return Err(ManagementError::unavailable(format!(
                "owner {owner} is unhealthy, in maintenance, or disk-ineligible"
            )));
        }
    }
    Ok(())
}

pub fn validate_request(request: &PreviewRequest) -> Result<(), ManagementError> {
    if !matches!(request.kind.as_str(), "topic" | "channel") {
        return Err(ManagementError::bad_request(
            "E_BAD_RESOURCE_KIND",
            "kind must be topic or channel",
        ));
    }
    if !matches!(
        request.action.as_str(),
        "create" | "pause" | "unpause" | "empty" | "delete" | "retry"
    ) {
        return Err(ManagementError::bad_request(
            "E_BAD_ACTION",
            "unsupported management action",
        ));
    }
    rustqueue_protocol::validate_name(&request.topic)
        .map_err(|_| ManagementError::bad_request("E_BAD_TOPIC", "invalid topic name"))?;
    if request.kind == "channel" {
        let channel = request.channel.as_deref().ok_or_else(|| {
            ManagementError::bad_request("E_BAD_CHANNEL", "channel name is required")
        })?;
        rustqueue_protocol::validate_name(channel)
            .map_err(|_| ManagementError::bad_request("E_BAD_CHANNEL", "invalid channel name"))?;
        if channel.ends_with("#ephemeral") {
            return Err(ManagementError::conflict(
                "E_EPHEMERAL_UNMANAGED",
                "ephemeral channels are observation-only",
            ));
        }
    } else if request.channel.is_some() {
        return Err(ManagementError::bad_request(
            "E_BAD_CHANNEL",
            "topic actions cannot include a channel",
        ));
    }
    Ok(())
}

fn resolve_target(
    snapshot: &Snapshot,
    request: &PreviewRequest,
) -> Result<ResolvedTarget, ManagementError> {
    let topic = snapshot
        .topics
        .iter()
        .find(|topic| topic.name == request.topic);
    if request.kind == "topic" {
        if request.action == "create" {
            if topic.is_some_and(|topic| topic.managed_phase != "TOMBSTONED") {
                return Err(ManagementError::conflict(
                    "E_ALREADY_EXISTS",
                    "topic already exists",
                ));
            }
            let owner = choose_broker(snapshot)?;
            return Ok(ResolvedTarget {
                owners: vec![owner.clone()],
                impact: Impact {
                    owners: vec![owner],
                    connections: snapshot.summary.connections,
                    warnings: vec!["topic_requires_channel".into()],
                    ..Impact::empty()
                },
            });
        }
        let topic = topic
            .ok_or_else(|| ManagementError::conflict("E_NOT_FOUND", "topic does not exist"))?;
        if request.action == "retry" && topic.managed_phase != "FAILED" {
            return Err(ManagementError::conflict(
                "E_RETRY_NOT_FAILED",
                "only a failed topic operation can be retried",
            ));
        }
        if request.action != "retry" && topic.managed_phase != "ACTIVE" {
            return Err(ManagementError::conflict(
                "E_OPERATION_IN_PROGRESS",
                "topic has another management operation in progress",
            ));
        }
        if topic
            .channels
            .iter()
            .any(|channel| !matches!(channel.managed_phase.as_str(), "ACTIVE" | "TOMBSTONED"))
        {
            return Err(ManagementError::conflict(
                "E_CHANNEL_OPERATION_IN_PROGRESS",
                "topic has an unfinished channel operation",
            ));
        }
        let destructive = is_destructive(&request.action);
        if destructive && topic.owners.len() > 1 {
            return Err(ManagementError::conflict(
                "E_MIGRATION_IN_PROGRESS",
                "destructive topic operations are blocked while multiple owners are visible",
            ));
        }
        let depth = topic.channels.iter().map(|channel| channel.depth).sum();
        let in_flight = topic.channels.iter().map(|channel| channel.in_flight).sum();
        let deferred = topic.channels.iter().map(|channel| channel.deferred).sum();
        return Ok(ResolvedTarget {
            owners: topic.owners.clone(),
            impact: Impact {
                owners: topic.owners.clone(),
                stored_messages: topic.stored_messages,
                depth,
                in_flight,
                deferred,
                connections: snapshot.summary.connections,
                warnings: if destructive {
                    vec!["immediate_irreversible".into()]
                } else {
                    Vec::new()
                },
            },
        });
    }

    let topic =
        topic.ok_or_else(|| ManagementError::conflict("E_NOT_FOUND", "topic does not exist"))?;
    if topic.managed_phase != "ACTIVE" {
        return Err(ManagementError::conflict(
            "E_TOPIC_OPERATION_IN_PROGRESS",
            "channel operations require an active parent topic",
        ));
    }
    let channel_name = request.channel.as_deref().expect("validated channel");
    let channel = topic
        .channels
        .iter()
        .find(|channel| channel.name == channel_name);
    if request.action == "create" {
        if channel.is_some_and(|channel| channel.managed_phase != "TOMBSTONED") {
            return Err(ManagementError::conflict(
                "E_ALREADY_EXISTS",
                "channel already exists",
            ));
        }
        return Ok(ResolvedTarget {
            owners: topic.owners.clone(),
            impact: Impact {
                owners: topic.owners.clone(),
                connections: snapshot.summary.connections,
                ..Impact::empty()
            },
        });
    }
    let channel = channel
        .ok_or_else(|| ManagementError::conflict("E_NOT_FOUND", "channel does not exist"))?;
    if request.action == "retry" && channel.managed_phase != "FAILED" {
        return Err(ManagementError::conflict(
            "E_RETRY_NOT_FAILED",
            "only a failed channel operation can be retried",
        ));
    }
    if request.action != "retry" && channel.managed_phase != "ACTIVE" {
        return Err(ManagementError::conflict(
            "E_OPERATION_IN_PROGRESS",
            "channel has another management operation in progress",
        ));
    }
    if channel.ephemeral {
        return Err(ManagementError::conflict(
            "E_EPHEMERAL_UNMANAGED",
            "ephemeral channels are observation-only",
        ));
    }
    if is_destructive(&request.action) && channel.owners.len() > 1 {
        return Err(ManagementError::conflict(
            "E_MIGRATION_IN_PROGRESS",
            "destructive channel operations are blocked while multiple owners are visible",
        ));
    }
    Ok(ResolvedTarget {
        owners: channel.owners.clone(),
        impact: Impact {
            owners: channel.owners.clone(),
            stored_messages: 0,
            depth: channel.depth,
            in_flight: channel.in_flight,
            deferred: channel.deferred,
            connections: snapshot.summary.connections,
            warnings: if is_destructive(&request.action) {
                vec!["immediate_irreversible".into()]
            } else {
                Vec::new()
            },
        },
    })
}

fn choose_broker(snapshot: &Snapshot) -> Result<String, ManagementError> {
    let ownership: BTreeMap<_, _> = snapshot
        .brokers
        .iter()
        .map(|broker| {
            let count = snapshot
                .topics
                .iter()
                .filter(|topic| topic.owners.contains(&broker.name))
                .count();
            (broker.name.clone(), count)
        })
        .collect();
    let mut candidates: Vec<&BrokerView> = snapshot
        .brokers
        .iter()
        .filter(|broker| {
            ensure_owners_healthy(snapshot, &[broker.name.clone()]).is_ok()
                && broker.observation.as_ref().is_some_and(|observation| {
                    !observation.disk.pressure
                        && observation.disk.available_bytes > observation.disk.min_free_bytes
                })
        })
        .collect();
    candidates.sort_by(|left, right| {
        ownership[&left.name]
            .cmp(&ownership[&right.name])
            .then_with(|| {
                let left_available = left
                    .observation
                    .as_ref()
                    .map(|value| value.disk.available_bytes)
                    .unwrap_or_default();
                let right_available = right
                    .observation
                    .as_ref()
                    .map(|value| value.disk.available_bytes)
                    .unwrap_or_default();
                right_available.cmp(&left_available)
            })
            .then_with(|| left.name.cmp(&right.name))
    });
    candidates
        .first()
        .map(|broker| broker.name.clone())
        .ok_or_else(|| ManagementError::unavailable("no healthy disk-eligible broker is available"))
}

async fn subject(state: &AppState, request: &PreviewRequest) -> Result<Subject, ManagementError> {
    let namespace = &state.config.namespace;
    let queue = &state.config.queue_name;
    if request.kind == "channel" {
        if let Some(channel) = resources::get_channel(
            &state.client,
            namespace,
            queue,
            &request.topic,
            request.channel.as_deref().expect("validated channel"),
        )
        .await
        .map_err(|error| ManagementError::unavailable(error.to_string()))?
        {
            return Ok(resource_subject("channel", &channel));
        }
    }
    if let Some(topic) = resources::get_topic(&state.client, namespace, queue, &request.topic)
        .await
        .map_err(|error| ManagementError::unavailable(error.to_string()))?
    {
        return Ok(resource_subject("topic", &topic));
    }
    let cluster = Api::<RustQueue>::namespaced(state.client.clone(), namespace)
        .get(queue)
        .await
        .map_err(|error| ManagementError::unavailable(error.to_string()))?;
    let catalog = resources::list(&state.client, namespace, queue)
        .await
        .map_err(|error| ManagementError::unavailable(error.to_string()))?;
    Ok(Subject {
        kind: "catalog".into(),
        uid: cluster.uid().unwrap_or_default(),
        resource_version: catalog.fences().revision,
    })
}

fn resource_subject<K: ResourceExt>(kind: &str, resource: &K) -> Subject {
    Subject {
        kind: kind.into(),
        uid: resource.uid().unwrap_or_default(),
        resource_version: resource.resource_version().unwrap_or_default(),
    }
}

pub fn is_destructive(action: &str) -> bool {
    matches!(action, "empty" | "delete")
}

fn requires_confirmation(action: &str) -> bool {
    is_destructive(action) || action == "retry"
}

struct ResolvedTarget {
    owners: Vec<String>,
    impact: Impact,
}

impl Impact {
    fn empty() -> Self {
        Self {
            owners: Vec::new(),
            stored_messages: 0,
            depth: 0,
            in_flight: 0,
            deferred: 0,
            connections: 0,
            warnings: Vec::new(),
        }
    }
}

struct Subject {
    kind: String,
    uid: String,
    resource_version: String,
}
