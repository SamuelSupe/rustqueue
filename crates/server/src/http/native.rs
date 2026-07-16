use super::*;

#[derive(Deserialize)]
pub(super) struct DrainRequest {
    #[serde(default = "enabled_by_default")]
    enabled: bool,
}

fn enabled_by_default() -> bool {
    true
}

pub(super) async fn health(State(state): State<AppState>) -> Response {
    if !state.accepting.load(Ordering::Acquire) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "not_ready", "reason": "draining", "node_id": state.config.node.id,
            })),
        )
            .into_response();
    }
    if !state.publish_admission.storage_ready() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "not_ready", "reason": "disk_pressure", "node_id": state.config.node.id,
            })),
        )
            .into_response();
    }
    if !state.broker.storage_healthy() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "not_ready", "reason": "storage_fault", "node_id": state.config.node.id,
            })),
        )
            .into_response();
    }
    Json(json!({
        "status": "ready", "node_id": state.config.node.id,
        "storage": "healthy", "mode": "share-nothing", "data_format": 7,
    }))
    .into_response()
}

pub(super) async fn registry(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, state.registry_token.as_deref(), "registry")?;
    let stats = state.broker.stats();
    let topics = registry_topics(&stats);
    let (stored_messages, depth, in_flight) = backlog(&stats);
    let process_ready = state.accepting.load(Ordering::Acquire);
    let storage_ready = state.broker.storage_healthy();
    let publish_ready = process_ready && storage_ready && state.publish_admission.storage_ready();
    let consume_ready =
        storage_ready && (process_ready || stored_messages > 0 || depth > 0 || in_flight > 0);
    let (binary, storage) = state.broker.capabilities();
    Ok(Json(json!({
        "format": 7,
        "revision": state.broker.registry_revision(),
        "node_id": state.config.node.id,
        "ready": consume_ready,
        "publish_ready": publish_ready,
        "consume_ready": consume_ready,
        "broadcast_address": state.config.node.broadcast_address,
        "tcp_port": state.config.network.advertised_tcp_port,
        "http_port": state.config.network.advertised_http_port,
        "stored_messages": stored_messages,
        "depth": depth,
        "in_flight": in_flight,
        "topics": topics,
        "compatibility": {"binary": binary, "storage": storage},
    })))
}

pub(super) async fn capabilities(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, state.registry_token.as_deref(), "registry")?;
    let (binary, storage) = state.broker.capabilities();
    Ok(Json(json!({"binary": binary, "storage": storage})))
}

pub(super) fn registry_topics(stats: &BrokerStats) -> Vec<Value> {
    stats
        .topics
        .iter()
        .map(|topic| {
            let channels: Vec<_> = topic
                .channels
                .iter()
                .map(|channel| channel.name.as_str())
                .collect();
            json!({
                "name": topic.name,
                "paused": topic.paused,
                "channels": channels,
                "stored_messages": topic.message_count,
            })
        })
        .collect()
}

pub(super) async fn drain_status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, state.registry_token.as_deref(), "registry")?;
    let stats = state.broker.stats();
    let (stored_messages, depth, in_flight) = backlog(&stats);
    Ok(Json(json!({
        "draining": !state.accepting.load(Ordering::Acquire),
        "drained": stored_messages == 0 && depth == 0 && in_flight == 0,
        "stored_messages": stored_messages,
        "depth": depth,
        "in_flight": in_flight,
    })))
}

pub(super) async fn set_drain(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<DrainRequest>,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, state.admin_token.as_deref(), "admin")?;
    state.accepting.store(!request.enabled, Ordering::Release);
    tracing::info!(enabled = request.enabled, "broker drain state changed");
    let stats = state.broker.stats();
    let (stored_messages, depth, in_flight) = backlog(&stats);
    Ok(Json(json!({
        "draining": request.enabled,
        "drained": stored_messages == 0 && depth == 0 && in_flight == 0,
        "stored_messages": stored_messages,
        "depth": depth,
        "in_flight": in_flight,
    })))
}

pub(super) async fn native_stats(
    State(state): State<AppState>,
    Query(query): Query<StatsQuery>,
) -> Json<Value> {
    let filtered = filter_stats(
        state.broker.stats(),
        query.topic.as_deref(),
        query.channel.as_deref(),
    );
    Json(json!({
        "complete": true,
        "node_id": state.config.node.id,
        "collected_at_ms": now_ms(),
        "topics": filtered.topics,
    }))
}

pub(super) async fn scrub(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, state.admin_token.as_deref(), "admin")?;
    let records = state.broker.scrub().await?;
    Ok(Json(json!({"status": "ok", "records_checked": records})))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
