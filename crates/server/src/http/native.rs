use super::*;

#[derive(Deserialize)]
pub(super) struct DrainRequest {
    #[serde(default = "enabled_by_default")]
    enabled: bool,
    #[serde(default)]
    freeze_deliveries: bool,
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
    if !state.broker.management_fences_ready() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "not_ready", "reason": "management_fences_stale", "node_id": state.config.node.id,
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
    authorize_any(
        &headers,
        &[&state.tokens.registry, &state.tokens.console],
        "registry or console",
    )?;
    state.broker.expire_in_flight().await?;
    let stats = state.broker.stats();
    let topics = registry_topics(&stats);
    let (stored_messages, depth, in_flight) = backlog(&stats);
    let process_ready = state.accepting.load(Ordering::Acquire);
    let storage_ready = state.broker.storage_healthy();
    let management_ready = state.broker.management_fences_ready();
    let publish_ready = process_ready
        && storage_ready
        && management_ready
        && state.publish_admission.storage_ready();
    let delivery_ready = state.delivering.load(Ordering::Acquire);
    let consume_ready = delivery_ready
        && management_ready
        && storage_ready
        && (process_ready || stored_messages > 0 || depth > 0 || in_flight > 0);
    let (_, storage) = state.broker.capabilities();
    let binary = crate::config::runtime_capabilities();
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

pub(super) async fn registry_head(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize_any(
        &headers,
        &[&state.tokens.registry, &state.tokens.console],
        "registry or console",
    )?;
    let process_ready = state.accepting.load(Ordering::Acquire);
    let delivery_ready = state.delivering.load(Ordering::Acquire);
    let storage_ready = state.broker.storage_healthy();
    let management_ready = state.broker.management_fences_ready();
    let publish_ready = process_ready
        && storage_ready
        && management_ready
        && state.publish_admission.storage_ready();
    Ok(Json(json!({
        "format": 7,
        "revision": state.broker.registry_revision(),
        "node_id": state.config.node.id,
        "ready": delivery_ready && storage_ready && management_ready,
        "publish_ready": publish_ready,
        "consume_ready": delivery_ready && storage_ready && management_ready,
    })))
}

pub(super) async fn capabilities(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize_any(
        &headers,
        &[&state.tokens.registry, &state.tokens.console],
        "registry or console",
    )?;
    let (_, storage) = state.broker.capabilities();
    let binary = crate::config::runtime_capabilities();
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
    authorize_any(
        &headers,
        &[&state.tokens.registry, &state.tokens.console],
        "registry or console",
    )?;
    state.broker.expire_in_flight().await?;
    let stats = state.broker.metrics_stats(false, 0);
    let (stored_messages, depth, in_flight) = backlog(&stats);
    let draining = !state.accepting.load(Ordering::Acquire);
    let delivery_frozen = !state.delivering.load(Ordering::Acquire);
    let publish_inflight_bytes = state.metrics.publish_inflight_bytes.load(Ordering::Acquire);
    let delivery_inflight_bytes = stats.delivery_budget.in_flight_bytes;
    let empty = stored_messages == 0
        && depth == 0
        && in_flight == 0
        && publish_inflight_bytes == 0
        && delivery_inflight_bytes == 0;
    Ok(Json(json!({
        "draining": draining,
        "delivery_frozen": delivery_frozen,
        "quiesced": draining && delivery_frozen && in_flight == 0
            && publish_inflight_bytes == 0 && delivery_inflight_bytes == 0,
        "empty": empty,
        "drained": empty,
        "stored_messages": stored_messages,
        "depth": depth,
        "in_flight": in_flight,
        "publish_inflight_bytes": publish_inflight_bytes,
        "delivery_inflight_bytes": delivery_inflight_bytes,
    })))
}

pub(super) async fn set_drain(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<DrainRequest>,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, &state.tokens.admin, "admin")?;
    state.accepting.store(!request.enabled, Ordering::Release);
    state.delivering.store(
        !request.enabled || !request.freeze_deliveries,
        Ordering::Release,
    );
    tracing::info!(
        enabled = request.enabled,
        freeze_deliveries = request.freeze_deliveries,
        "broker drain state changed"
    );
    state.broker.expire_in_flight().await?;
    let stats = state.broker.metrics_stats(false, 0);
    let (stored_messages, depth, in_flight) = backlog(&stats);
    let publish_inflight_bytes = state.metrics.publish_inflight_bytes.load(Ordering::Acquire);
    let delivery_frozen = !state.delivering.load(Ordering::Acquire);
    let delivery_inflight_bytes = stats.delivery_budget.in_flight_bytes;
    let empty = stored_messages == 0
        && depth == 0
        && in_flight == 0
        && publish_inflight_bytes == 0
        && delivery_inflight_bytes == 0;
    Ok(Json(json!({
        "draining": request.enabled,
        "delivery_frozen": delivery_frozen,
        "quiesced": request.enabled && delivery_frozen && in_flight == 0
            && publish_inflight_bytes == 0 && delivery_inflight_bytes == 0,
        "empty": empty,
        "drained": empty,
        "stored_messages": stored_messages,
        "depth": depth,
        "in_flight": in_flight,
        "publish_inflight_bytes": publish_inflight_bytes,
        "delivery_inflight_bytes": delivery_inflight_bytes,
    })))
}

pub(super) async fn native_stats(
    State(state): State<AppState>,
    Query(query): Query<StatsQuery>,
) -> Result<Json<Value>, ApiError> {
    state.broker.expire_in_flight().await?;
    let filtered = state
        .broker
        .filtered_stats(query.topic.as_deref(), query.channel.as_deref());
    Ok(Json(json!({
        "complete": true,
        "node_id": state.config.node.id,
        "collected_at_ms": now_ms(),
        "topics": filtered.topics,
    })))
}

pub(super) async fn observe(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, &state.tokens.console, "console")?;
    state.broker.expire_in_flight().await?;
    let stats = state.broker.stats();
    let segment_count = stats.aggregate.segment_count;
    let segment_bytes = stats.aggregate.segment_bytes;
    let mut value = observation_head(&state);
    let object = value
        .as_object_mut()
        .expect("observation head is an object");
    object.insert(
        "storage".into(),
        json!({"segment_count": segment_count, "segment_bytes": segment_bytes}),
    );
    object.insert("catalog_collected_at_ms".into(), json!(now_ms()));
    object.insert(
        "queue".into(),
        serde_json::to_value(stats).expect("stats serialize"),
    );
    Ok(Json(value))
}

pub(super) async fn observe_head(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, &state.tokens.console, "console")?;
    Ok(Json(observation_head(&state)))
}

fn observation_head(state: &AppState) -> Value {
    let process_ready = state.accepting.load(Ordering::Acquire);
    let delivery_ready = state.delivering.load(Ordering::Acquire);
    let storage_healthy = state.broker.storage_healthy();
    let disk_ready = state.publish_admission.storage_ready();
    let management_ready = state.broker.management_fences_ready();
    let (_, storage) = state.broker.capabilities();
    let binary = crate::config::runtime_capabilities();
    let runtime = state.metrics.snapshot();
    json!({
        "schema_version": 1,
        "registry_revision": state.broker.registry_revision(),
        "collected_at_ms": now_ms(),
        "node": {
            "id": state.config.node.id,
            "address": state.config.node.broadcast_address,
            "version": env!("CARGO_PKG_VERSION"),
            "data_format": 7,
            "compatibility": {"binary": binary, "storage": storage},
        },
        "readiness": {
            "process_ready": process_ready,
            "storage_healthy": storage_healthy,
            "disk_ready": disk_ready,
            "publish_ready": process_ready && storage_healthy && disk_ready && management_ready,
            "consume_ready": delivery_ready && storage_healthy && management_ready,
            "draining": !process_ready,
            "management_fences_ready": management_ready,
        },
        "disk": {
            "total_bytes": runtime.disk_total_bytes,
            "available_bytes": runtime.disk_available_bytes,
            "used_percent": runtime.disk_used_percent,
            "pressure": runtime.disk_pressure != 0,
            "high_watermark_percent": state.config.storage.disk_high_watermark_percent,
            "low_watermark_percent": state.config.storage.disk_low_watermark_percent,
            "min_free_bytes": state.config.storage.min_free_bytes,
            "protective_eviction_enabled": state.config.storage.protective_eviction_enabled,
        },
        "runtime": runtime,
        "delivery_budget": state.broker.delivery_budget_stats(),
        "limits": {
            "max_message_bytes": state.config.queue.max_message_bytes,
            "publish_ack_mode": state.config.queue.publish_ack_mode.as_str(),
            "relaxed_sync_messages": state.config.queue.relaxed_sync_messages,
            "relaxed_sync_bytes": state.config.queue.relaxed_sync_bytes,
            "relaxed_sync_interval_ms": state.config.queue.relaxed_sync_interval_ms,
            "message_index_cache_bytes": state.config.storage.message_index_cache_bytes,
            "max_connections": state.config.limits.max_connections,
        },
    })
}

pub(super) async fn scrub(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, &state.tokens.admin, "admin")?;
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
