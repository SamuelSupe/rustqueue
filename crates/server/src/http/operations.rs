use super::{authorize, ApiError, AppState};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use rustqueue_consensus::{MaintenanceLease, OperationKind, OperationState, QueueCommand};
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Deserialize)]
struct ExpandRequest {
    target_partitions: u16,
}

#[derive(Deserialize)]
struct AutomationRequest {
    enabled: bool,
}

#[derive(Deserialize)]
struct MaintenanceRequest {
    enabled: bool,
    ttl_seconds: Option<u64>,
    #[serde(default)]
    reason: String,
}

pub(super) fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/topics/{topic}/partitions", post(expand_partitions))
        .route("/v1/cluster/nodes", get(nodes))
        .route("/v1/cluster/automation", post(set_automation))
        .route("/v1/cluster/rebalance/plan", get(rebalance_plan))
        .route("/v1/cluster/rebalance/run", post(run_rebalance))
        .route("/v1/cluster/operations", get(operations))
        .route("/v1/cluster/operations/{operation_id}", get(operation))
        .route(
            "/v1/cluster/operations/{operation_id}/pause",
            post(pause_operation),
        )
        .route(
            "/v1/cluster/operations/{operation_id}/resume",
            post(resume_operation),
        )
        .route(
            "/v1/cluster/operations/{operation_id}/cancel",
            post(cancel_operation),
        )
        .route(
            "/v1/cluster/nodes/{node_id}/maintenance",
            post(set_maintenance),
        )
}

async fn rebalance_plan(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize_admin(&state, &headers)?;
    let plan = cluster(&state)?.rebalance_plan();
    Ok(Json(json!({ "moves": plan })))
}

async fn run_rebalance(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    authorize_admin(&state, &headers)?;
    let operation_ids = cluster(&state)?
        .run_rebalance_plan()
        .await
        .map_err(cluster_error)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "operation_ids": operation_ids })),
    )
        .into_response())
}

async fn expand_partitions(
    State(state): State<AppState>,
    Path(topic): Path<String>,
    headers: HeaderMap,
    Json(request): Json<ExpandRequest>,
) -> Result<Response, ApiError> {
    authorize_admin(&state, &headers)?;
    let cluster = state.consensus.as_ref().ok_or_else(cluster_disabled)?;
    let current = if cluster.control_plane_enabled() {
        cluster
            .catalog_topic_descriptor(&topic)
            .await
            .map_err(|error| conflict(error.to_string()))?
            .map(|descriptor| {
                descriptor
                    .partitions
                    .values()
                    .filter(|partition| {
                        partition.lifecycle == rustqueue_consensus::PartitionHomeLifecycle::Active
                    })
                    .count() as u16
            })
    } else {
        cluster.metadata().topic(&topic).map(|descriptor| {
            descriptor
                .partitions
                .iter()
                .filter(|partition| {
                    partition.lifecycle == rustqueue_consensus::PartitionLifecycle::Active
                })
                .count() as u16
        })
    };
    if let Some(current) = current {
        if request.target_partitions == current {
            return Ok(Json(json!({
                "status": "completed",
                "topic": topic,
                "current_partitions": current,
                "target_partitions": current,
            }))
            .into_response());
        }
        if request.target_partitions < current {
            return Err(conflict("partition count can only be increased"));
        }
    }
    let operation = cluster
        .expand_partitions(
            &topic,
            request.target_partitions,
            state.config.queue.max_partitions_per_topic,
        )
        .await
        .map_err(|error| conflict(error.to_string()))?;
    let runtime = cluster.clone();
    tokio::spawn(async move {
        if let Err(error) = runtime.reconcile_partition_expansions().await {
            tracing::warn!(%error, "partition expansion reconciliation will retry");
        }
    });
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({
            "status": operation.state,
            "operation_id": operation.id,
            "phase": operation.phase,
            "kind": operation.kind,
        })),
    )
        .into_response())
}

async fn operations(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize_admin(&state, &headers)?;
    let operations = cluster(&state)?.metadata().operations();
    Ok(Json(json!({ "operations": operations })))
}

async fn operation(
    State(state): State<AppState>,
    Path(operation_id): Path<u64>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize_admin(&state, &headers)?;
    let operation = cluster(&state)?
        .metadata()
        .operation(operation_id)
        .ok_or_else(|| ApiError {
            status: StatusCode::NOT_FOUND,
            code: "E_OPERATION",
            detail: "operation not found".into(),
        })?;
    Ok(Json(json!(operation)))
}

async fn pause_operation(
    State(state): State<AppState>,
    Path(operation_id): Path<u64>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    set_paused(&state, &headers, operation_id, true).await
}

async fn resume_operation(
    State(state): State<AppState>,
    Path(operation_id): Path<u64>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    set_paused(&state, &headers, operation_id, false).await
}

async fn set_paused(
    state: &AppState,
    headers: &HeaderMap,
    operation_id: u64,
    paused: bool,
) -> Result<Json<Value>, ApiError> {
    authorize_admin(state, headers)?;
    let response = cluster(state)?
        .write(QueueCommand::SetOperationPaused {
            operation_id,
            paused,
        })
        .await
        .map_err(cluster_error)?;
    if let Some(error) = response.error {
        return Err(conflict(error));
    }
    tracing::info!(
        audit_event = "operation_pause_changed",
        operation_id,
        paused,
        "maintenance operation pause changed"
    );
    Ok(Json(json!({
        "operation_id": operation_id,
        "state": if paused { "paused" } else { "running" },
    })))
}

async fn cancel_operation(
    State(state): State<AppState>,
    Path(operation_id): Path<u64>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize_admin(&state, &headers)?;
    let runtime = cluster(&state)?;
    let operation = runtime
        .metadata()
        .operation(operation_id)
        .ok_or_else(|| conflict("operation not found"))?;
    match operation.kind {
        OperationKind::ExpandPartitions { .. } => runtime
            .cancel_expansion(operation_id)
            .await
            .map_err(|error| conflict(error.to_string()))?,
        _ => {
            return Err(conflict(
                "operation cannot be cancelled in its current phase",
            ))
        }
    }
    Ok(Json(json!({
        "operation_id": operation_id,
        "state": OperationState::Cancelled,
    })))
}

async fn set_automation(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<AutomationRequest>,
) -> Result<Json<Value>, ApiError> {
    authorize_admin(&state, &headers)?;
    let response = cluster(&state)?
        .write(QueueCommand::SetAutomationEnabled {
            enabled: request.enabled,
        })
        .await
        .map_err(cluster_error)?;
    if let Some(error) = response.error {
        return Err(conflict(error));
    }
    tracing::info!(
        audit_event = "automation_toggled",
        enabled = request.enabled,
        "cluster automation setting changed"
    );
    Ok(Json(json!({ "enabled": request.enabled })))
}

async fn set_maintenance(
    State(state): State<AppState>,
    Path(node_id): Path<u64>,
    headers: HeaderMap,
    Json(request): Json<MaintenanceRequest>,
) -> Result<Json<Value>, ApiError> {
    authorize_admin(&state, &headers)?;
    let config = &state.config.cluster.shutdown;
    let lease = if request.enabled {
        let ttl = request
            .ttl_seconds
            .unwrap_or(config.maintenance_default_ttl_seconds);
        if ttl == 0 || ttl > config.maintenance_max_ttl_seconds {
            return Err(ApiError::bad_request(
                "E_MAINTENANCE",
                "maintenance TTL is outside the configured range",
            ));
        }
        Some(MaintenanceLease {
            expires_at_ms: now_ms().saturating_add((ttl * 1000).min(i64::MAX as u64) as i64),
            reason: request.reason,
        })
    } else {
        None
    };
    let response = cluster(&state)?
        .write(QueueCommand::SetMaintenance {
            node_id,
            lease: lease.clone(),
        })
        .await
        .map_err(cluster_error)?;
    if let Some(error) = response.error {
        return Err(conflict(error));
    }
    if request.enabled && node_id == state.config.node.id {
        cluster(&state)?.evacuate_local_leaders().await;
    }
    tracing::info!(
        audit_event = "node_maintenance_changed",
        node_id,
        enabled = request.enabled,
        "node maintenance lease changed"
    );
    Ok(Json(json!({
        "node_id": node_id,
        "maintenance": lease,
    })))
}

async fn nodes(State(state): State<AppState>, headers: HeaderMap) -> Result<Json<Value>, ApiError> {
    authorize_admin(&state, &headers)?;
    let runtime = cluster(&state)?;
    let metadata = runtime.metadata().snapshot();
    let healthy = runtime.healthy_node_ids().await;
    let nodes: Vec<_> = metadata
        .nodes
        .into_values()
        .map(|node| {
            let maintenance = metadata.maintenance_nodes.get(&node.id).cloned();
            json!({
                "node": node,
                "healthy": healthy.contains(&node.id),
                "drained": metadata.drained_nodes.contains(&node.id),
                "maintenance": maintenance,
                "disk_used_percent": metadata.node_health.get(&node.id).map(|health| health.disk_used_percent),
                "disk_free_bytes": metadata.node_health.get(&node.id).map(|health| health.disk_free_bytes),
                "storage_eligible": metadata.node_health.get(&node.id).is_some_and(|health| health.storage_eligible),
            })
        })
        .collect();
    Ok(Json(json!({
        "automation_enabled": metadata.automation_enabled,
        "nodes": nodes,
    })))
}

fn authorize_admin(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    authorize(headers, state.admin_token.as_deref(), "admin")
}

fn cluster(
    state: &AppState,
) -> Result<&std::sync::Arc<rustqueue_consensus::ClusterRuntime>, ApiError> {
    state.consensus.as_ref().ok_or_else(cluster_disabled)
}

fn cluster_disabled() -> ApiError {
    ApiError {
        status: StatusCode::CONFLICT,
        code: "E_CLUSTER_DISABLED",
        detail: "cluster administration requires Raft mode".into(),
    }
}

fn conflict(detail: impl Into<String>) -> ApiError {
    ApiError {
        status: StatusCode::CONFLICT,
        code: "E_CONFLICT",
        detail: detail.into(),
    }
}

fn cluster_error(error: anyhow::Error) -> ApiError {
    ApiError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        code: "E_CLUSTER",
        detail: error.to_string(),
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
