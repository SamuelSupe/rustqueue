use super::event;
use super::preview::{
    current_snapshot, ensure_owners_healthy, is_destructive, validate_request, PreviewRequest,
};
use super::resource_state;
use super::security::authorize_mutation;
use super::{ensure_enabled, ManagementError};
use crate::app::AppState;
use crate::session::{now_ms, ActionChallenge};
use axum::extract::{ConnectInfo, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::Json;
use serde::Deserialize;
use serde_json::json;
use std::net::SocketAddr;

#[derive(Clone, Debug, Deserialize)]
pub struct ApplyRequest {
    pub(super) kind: String,
    pub(super) action: String,
    pub(super) topic: String,
    pub(super) channel: Option<String>,
    pub(super) action_token: String,
    #[serde(default)]
    pub(super) confirmation: String,
}

pub async fn apply(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(request): Json<ApplyRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), ManagementError> {
    ensure_enabled(&state)?;
    let (session_id, _) = authorize_mutation(&state, &headers)?;
    let preview_request = PreviewRequest {
        kind: request.kind.clone(),
        action: request.action.clone(),
        topic: request.topic.clone(),
        channel: request.channel.clone(),
    };
    validate_request(&preview_request)?;
    let challenge = state
        .sessions
        .take_challenge(&session_id, &request.action_token)
        .ok_or_else(|| {
            ManagementError::conflict(
                "E_ACTION_TOKEN_EXPIRED",
                "action token is expired, invalid, or already used",
            )
        })?;
    verify_request(&request, &challenge)?;
    let _guard = state.mutation_lock.lock().await;
    if challenge.expires_at_ms <= now_ms() {
        return Err(ManagementError::conflict(
            "E_ACTION_TOKEN_EXPIRED",
            "action token expired while waiting for the management lock",
        ));
    }
    let snapshot = current_snapshot(&state)?;
    resource_state::verify_subject(&state, &challenge).await?;
    verify_current_owners(&snapshot, &request, &challenge)?;
    ensure_owners_healthy(&snapshot, &challenge.owners)?;

    let action_name = format!("{}.{}", request.kind, request.action);
    let target = request
        .channel
        .as_ref()
        .map(|channel| format!("{}/{channel}", request.topic))
        .unwrap_or_else(|| request.topic.clone());
    let user_agent = headers
        .get(header::USER_AGENT)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("unknown")
        .chars()
        .take(256)
        .collect::<String>();
    let result = execute(&state, &request, &challenge).await;
    match result {
        Ok(operation) => {
            tracing::info!(
                source_ip = %peer.ip(),
                user_agent = %user_agent,
                action = %action_name,
                result = "accepted",
                "console management audit"
            );
            event::emit(
                &state,
                &action_name,
                &target,
                "accepted",
                "operation persisted for reconciliation",
            )
            .await;
            Ok((
                StatusCode::ACCEPTED,
                Json(json!({
                    "status": "accepted",
                    "operation_id": operation.id,
                    "resource_revision": operation.revision,
                    "refresh_required": true,
                })),
            ))
        }
        Err(error) => {
            tracing::warn!(
                source_ip = %peer.ip(),
                user_agent = %user_agent,
                action = %action_name,
                result = "failed",
                "console management audit"
            );
            event::emit(&state, &action_name, &target, "failed", error.detail()).await;
            Err(error)
        }
    }
}

async fn execute(
    state: &AppState,
    request: &ApplyRequest,
    challenge: &ActionChallenge,
) -> Result<resource_state::StartedOperation, ManagementError> {
    let tombstone_until_ms = matches!(request.action.as_str(), "delete" | "retry").then(|| {
        now_ms()
            .saturating_add(state.config.tombstone_ttl.as_millis() as u64)
            .min(i64::MAX as u64) as i64
    });
    if request.kind == "topic" {
        resource_state::begin_topic(state, request, challenge, tombstone_until_ms).await
    } else {
        resource_state::begin_channel(state, request, challenge, tombstone_until_ms).await
    }
}

fn verify_request(
    request: &ApplyRequest,
    challenge: &ActionChallenge,
) -> Result<(), ManagementError> {
    if request.kind != challenge.kind
        || request.action != challenge.action
        || request.topic != challenge.topic
        || request.channel != challenge.channel
    {
        return Err(ManagementError::conflict(
            "E_ACTION_TOKEN_MISMATCH",
            "action token is bound to a different resource or action",
        ));
    }
    if challenge
        .confirmation
        .as_ref()
        .is_some_and(|expected| request.confirmation != *expected)
    {
        return Err(ManagementError::bad_request(
            "E_CONFIRMATION_MISMATCH",
            "typed resource name does not match",
        ));
    }
    Ok(())
}

fn verify_current_owners(
    snapshot: &crate::model::Snapshot,
    request: &ApplyRequest,
    challenge: &ActionChallenge,
) -> Result<(), ManagementError> {
    let topic = snapshot
        .topics
        .iter()
        .find(|topic| topic.name == request.topic);
    let mut current = if request.kind == "topic" && request.action == "create" {
        challenge.owners.clone()
    } else if request.kind == "channel" && request.action == "create" {
        topic.map(|topic| topic.owners.clone()).unwrap_or_default()
    } else if request.kind == "topic" {
        topic.map(|topic| topic.owners.clone()).unwrap_or_default()
    } else {
        topic
            .and_then(|topic| {
                topic
                    .channels
                    .iter()
                    .find(|channel| request.channel.as_deref() == Some(channel.name.as_str()))
            })
            .map(|channel| channel.owners.clone())
            .unwrap_or_default()
    };
    let mut expected = challenge.owners.clone();
    current.sort();
    expected.sort();
    if current != expected {
        return Err(ManagementError::conflict(
            "E_RESOURCE_CHANGED",
            "resource ownership changed after preview",
        ));
    }
    if is_destructive(&request.action) && current.len() > 1 {
        return Err(ManagementError::conflict(
            "E_MIGRATION_IN_PROGRESS",
            "destructive operations are blocked while multiple owners are visible",
        ));
    }
    Ok(())
}
