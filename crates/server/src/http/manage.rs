use super::*;
use axum::extract::Path;
use rustqueue_queue::{ChannelManagementAction, ManagementFenceSnapshot, TopicManagementAction};

#[derive(Debug, Deserialize)]
pub(super) struct TopicManageRequest {
    operation_id: String,
    topic: String,
    expected_revision: u64,
    tombstone_until_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ChannelManageRequest {
    operation_id: String,
    topic: String,
    channel: String,
    expected_revision: u64,
    tombstone_until_ms: Option<i64>,
}

pub(super) async fn manage_topic(
    State(state): State<AppState>,
    Path(action): Path<String>,
    headers: HeaderMap,
    Json(request): Json<TopicManageRequest>,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, state.console_token.as_deref(), "console")?;
    let action = parse_topic_action(&action)?;
    let result = state
        .broker
        .manage_topic(
            &request.operation_id,
            &request.topic,
            action,
            request.expected_revision,
            request.tombstone_until_ms,
        )
        .await?;
    tracing::info!(
        target = %request.topic,
        action = ?action,
        changed = result.changed,
        revision = result.revision,
        "console topic management applied"
    );
    Ok(Json(json!(result)))
}

pub(super) async fn manage_channel(
    State(state): State<AppState>,
    Path(action): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ChannelManageRequest>,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, state.console_token.as_deref(), "console")?;
    let action = parse_channel_action(&action)?;
    let result = state
        .broker
        .manage_channel(
            &request.operation_id,
            &request.topic,
            &request.channel,
            action,
            request.expected_revision,
            request.tombstone_until_ms,
        )
        .await?;
    tracing::info!(
        target = %format!("{}/{}", request.topic, request.channel),
        action = ?action,
        changed = result.changed,
        revision = result.revision,
        "console channel management applied"
    );
    Ok(Json(json!(result)))
}

pub(super) async fn sync_fences(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(snapshot): Json<ManagementFenceSnapshot>,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, state.console_token.as_deref(), "console")?;
    state.broker.sync_management_fences(snapshot).await?;
    Ok(Json(json!({"status": "ok"})))
}

fn parse_topic_action(value: &str) -> Result<TopicManagementAction, ApiError> {
    match value {
        "create" => Ok(TopicManagementAction::Create),
        "pause" => Ok(TopicManagementAction::Pause),
        "unpause" => Ok(TopicManagementAction::Unpause),
        "empty" => Ok(TopicManagementAction::Empty),
        "delete" => Ok(TopicManagementAction::Delete),
        "tombstone" => Ok(TopicManagementAction::Tombstone),
        _ => Err(ApiError::bad_request(
            "E_BAD_ACTION",
            "unknown topic action",
        )),
    }
}

fn parse_channel_action(value: &str) -> Result<ChannelManagementAction, ApiError> {
    match value {
        "create" => Ok(ChannelManagementAction::Create),
        "pause" => Ok(ChannelManagementAction::Pause),
        "unpause" => Ok(ChannelManagementAction::Unpause),
        "empty" => Ok(ChannelManagementAction::Empty),
        "delete" => Ok(ChannelManagementAction::Delete),
        "tombstone" => Ok(ChannelManagementAction::Tombstone),
        _ => Err(ApiError::bad_request(
            "E_BAD_ACTION",
            "unknown channel action",
        )),
    }
}
