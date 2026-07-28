use super::*;
use crate::subscriptions::DeletePermit;
use axum::extract::Path;
use rustqueue_queue::{
    ChannelManagementAction, ChannelManagementCommand, ManagementFenceSnapshot, ManagementResult,
    TopicManagementAction,
};

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
    authorize(&headers, &state.tokens.console, "console")?;
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
    let cleanup_enabled = state.config.security.kodo_cleanup_enabled;
    apply_channel_management(state, &action, headers, request, cleanup_enabled).await
}

pub(super) async fn delete_idle_channel(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ChannelManageRequest>,
) -> Result<Json<Value>, ApiError> {
    apply_channel_management(state, "delete-if-idle", headers, request, true).await
}

pub(super) async fn delete_idle_channel_compat(
    State(state): State<AppState>,
    Query(query): Query<ChannelQuery>,
) -> Response {
    if query.topic.is_empty() || query.channel.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "E_BAD_REQUEST topic and channel are required",
        )
            .into_response();
    }
    let permit = match state
        .subscriptions
        .begin_delete(&query.topic, &query.channel)
    {
        Ok(permit) => permit,
        Err(blocked) => {
            return ApiError::conflict(
                "E_CHANNEL_NOT_IDLE",
                format!("channel deletion is blocked: {blocked:?}"),
            )
            .into_response()
        }
    };
    let revision = state.broker.registry_revision();
    let operation_id = kodo_compat_operation_id(revision, &query.topic, &query.channel);
    let result = manage_idle_channel(
        Arc::clone(&state.broker),
        permit,
        operation_id,
        query.topic.clone(),
        query.channel.clone(),
        revision,
        kodo_cleanup_deadline(),
    )
    .await;
    match result {
        Ok(result) => {
            tracing::info!(
                target = %format!("{}/{}", query.topic, query.channel),
                changed = result.changed,
                revision = result.revision,
                "Kodo compatibility channel cleanup applied"
            );
            "OK".into_response()
        }
        Err(BrokerError::TopicNotFound | BrokerError::ChannelNotFound) => {
            (StatusCode::NOT_FOUND, "E_NOT_FOUND CHANNEL_NOT_FOUND").into_response()
        }
        Err(error) => ApiError::from(error).into_response(),
    }
}

async fn apply_channel_management(
    state: AppState,
    action: &str,
    headers: HeaderMap,
    request: ChannelManageRequest,
    cleanup_enabled: bool,
) -> Result<Json<Value>, ApiError> {
    let (action, require_idle) = parse_channel_action(action, cleanup_enabled)?;
    if require_idle {
        authorize(&headers, &state.tokens.kodo_cleanup, "Kodo cleanup")?;
    } else {
        authorize(&headers, &state.tokens.console, "console")?;
    }
    let delete_permit = if require_idle {
        Some(
            state
                .subscriptions
                .begin_delete(&request.topic, &request.channel)
                .map_err(|blocked| {
                    ApiError::conflict(
                        "E_CHANNEL_NOT_IDLE",
                        format!("channel deletion is blocked: {blocked:?}"),
                    )
                })?,
        )
    } else {
        None
    };
    let tombstone_until_ms = if require_idle {
        Some(kodo_cleanup_deadline())
    } else {
        request.tombstone_until_ms
    };
    let result = if let Some(permit) = delete_permit {
        manage_idle_channel(
            Arc::clone(&state.broker),
            permit,
            request.operation_id.clone(),
            request.topic.clone(),
            request.channel.clone(),
            request.expected_revision,
            tombstone_until_ms.expect("idle deletion always has a server deadline"),
        )
        .await?
    } else {
        state
            .broker
            .manage_channel(ChannelManagementCommand {
                operation_id: &request.operation_id,
                topic: &request.topic,
                channel: &request.channel,
                action,
                expected_revision: request.expected_revision,
                tombstone_until_ms,
                require_idle,
            })
            .await?
    };
    tracing::info!(
        target = %format!("{}/{}", request.topic, request.channel),
        action = ?action,
        changed = result.changed,
        revision = result.revision,
        "channel management applied"
    );
    Ok(Json(json!(result)))
}

async fn manage_idle_channel(
    broker: Arc<Broker>,
    delete_permit: DeletePermit,
    operation_id: String,
    topic: String,
    channel: String,
    expected_revision: u64,
    tombstone_until_ms: i64,
) -> Result<ManagementResult, BrokerError> {
    let task = tokio::spawn(async move {
        let result = broker
            .manage_channel(ChannelManagementCommand {
                operation_id: &operation_id,
                topic: &topic,
                channel: &channel,
                action: ChannelManagementAction::Delete,
                expected_revision,
                tombstone_until_ms: Some(tombstone_until_ms),
                require_idle: true,
            })
            .await;
        drop(delete_permit);
        result
    });
    match task.await {
        Ok(result) => result,
        Err(error) => {
            tracing::error!(%error, "idle channel management task failed");
            Err(BrokerError::StorageUnavailable)
        }
    }
}

fn kodo_cleanup_deadline() -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64;
    now.saturating_add(10 * 60 * 1_000)
}

fn kodo_compat_operation_id(revision: u64, topic: &str, channel: &str) -> String {
    let target = format!("{topic}\0{channel}");
    format!(
        "kodo-compat-{revision:016x}-{:08x}",
        crc32c::crc32c(target.as_bytes())
    )
}

pub(super) async fn sync_fences(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(snapshot): Json<ManagementFenceSnapshot>,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, &state.tokens.console, "console")?;
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

fn parse_channel_action(
    value: &str,
    cleanup_enabled: bool,
) -> Result<(ChannelManagementAction, bool), ApiError> {
    match value {
        "create" => Ok((ChannelManagementAction::Create, false)),
        "pause" => Ok((ChannelManagementAction::Pause, false)),
        "unpause" => Ok((ChannelManagementAction::Unpause, false)),
        "empty" => Ok((ChannelManagementAction::Empty, false)),
        "delete" => Ok((ChannelManagementAction::Delete, false)),
        "delete-if-idle" if cleanup_enabled => Ok((ChannelManagementAction::Delete, true)),
        "tombstone" => Ok((ChannelManagementAction::Tombstone, false)),
        _ => Err(ApiError::bad_request(
            "E_BAD_ACTION",
            "unknown channel action",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscriptions::{ClientIdentity, RegisterBlocked, SubscriptionRegistry};
    use rustqueue_queue::BrokerConfig;
    use tempfile::tempdir;

    #[test]
    fn idle_delete_action_is_unavailable_until_kodo_cleanup_is_enabled() {
        assert!(parse_channel_action("delete-if-idle", false).is_err());
        assert!(matches!(
            parse_channel_action("delete-if-idle", true),
            Ok((ChannelManagementAction::Delete, true))
        ));
    }

    #[test]
    fn cleanup_deadline_is_server_bounded() {
        let deadline = kodo_cleanup_deadline();
        let now = super::unix_seconds() * 1_000;
        assert!((now + 9 * 60 * 1_000..=now + 11 * 60 * 1_000).contains(&deadline));
    }

    #[test]
    fn compatibility_cleanup_operation_ids_include_the_full_target() {
        assert_ne!(
            kodo_compat_operation_id(7, "a:b", "c"),
            kodo_compat_operation_id(7, "a", "b:c")
        );
        assert_ne!(
            kodo_compat_operation_id(7, "events", "workers"),
            kodo_compat_operation_id(8, "events", "workers")
        );
    }

    #[tokio::test]
    async fn cancelled_idle_delete_keeps_new_subscriptions_blocked_until_completion() {
        let root = tempdir().unwrap();
        let broker = Arc::new(
            Broker::open(BrokerConfig {
                data_path: root.path().to_path_buf(),
                ..BrokerConfig::default()
            })
            .unwrap(),
        );
        broker.create_channel("events", "workers").await.unwrap();
        let subscriptions = SubscriptionRegistry::default();
        let permit = subscriptions.begin_delete("events", "workers").unwrap();
        let revision = broker.registry_revision();
        let mut deletion = Box::pin(manage_idle_channel(
            Arc::clone(&broker),
            permit,
            "cancelled-idle-delete-0001".into(),
            "events".into(),
            "workers".into(),
            revision,
            kodo_cleanup_deadline(),
        ));
        std::future::poll_fn(|context| {
            assert!(
                std::future::Future::poll(deletion.as_mut(), context).is_pending(),
                "the detached deletion must not complete in its first poll"
            );
            std::task::Poll::Ready(())
        })
        .await;
        drop(deletion);

        assert!(matches!(
            subscriptions.register("events", "workers", ClientIdentity::default()),
            Err(RegisterBlocked::DeleteInProgress)
        ));

        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let channel_removed = broker
                    .channel_names("events")
                    .is_ok_and(|channels| channels.is_empty());
                let barrier_released = subscriptions.begin_delete("events", "workers").is_ok();
                if channel_removed && barrier_released {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
