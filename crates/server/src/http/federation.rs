use super::*;
use axum::extract::Path;
use futures::{stream, StreamExt};
use rustqueue_consensus::{CellId, PartitionMigrationPhase, RouteError};
use std::collections::{BTreeMap, BTreeSet};

const STATS_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Default, Deserialize)]
struct RouteQuery {
    topic: String,
    key: Option<String>,
    partition: Option<u16>,
    operation_id: Option<u64>,
}

#[derive(Deserialize)]
struct NativePublishQuery {
    topic: String,
    partition: u16,
    routing_epoch: u64,
    topology_generation: u64,
    #[serde(default)]
    defer: u64,
}

#[derive(Deserialize)]
struct NativeMultiPublishQuery {
    topic: String,
    partition: u16,
    routing_epoch: u64,
    topology_generation: u64,
    #[serde(default)]
    defer: u64,
    #[serde(default)]
    binary: bool,
}

#[derive(Deserialize)]
struct StartMigrationRequest {
    topic: String,
    partition: u16,
    target_cell_id: u64,
}

pub(super) fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/federation", get(describe))
        .route("/v1/federation/stats", get(stats))
        .route("/v1/federation/route", get(route))
        .route("/v1/federation/catalog", get(catalog))
        .route("/v1/federation/operations", get(operations))
        .route("/v1/federation/migrations", post(start_migration))
        .route(
            "/v1/federation/migrations/{operation_id}/resume",
            post(resume_migration),
        )
        .route("/v1/pub", post(native_publish))
        .route("/v1/mpub", post(native_multi_publish))
}

async fn native_publish(
    State(state): State<AppState>,
    Query(query): Query<NativePublishQuery>,
    headers: HeaderMap,
    request: Request<Body>,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, state.publish_token.as_deref(), "publish")?;
    validate_defer(query.defer, &state.config)?;
    validate_native_route(
        &state,
        &query.topic,
        query.partition,
        query.routing_epoch,
        query.topology_generation,
    )
    .await?;
    let (body, _reservation) =
        read_publish_body(&state, request, state.config.queue.max_message_bytes).await?;
    if body.is_empty() {
        return Err(ApiError::bad_request("E_BAD_MESSAGE", "message is empty"));
    }
    let bytes = body.len();
    let message_ids = publish_write(
        &state,
        &query.topic,
        vec![body],
        query.defer,
        Some(query.partition),
        None,
    )
    .await?;
    state
        .metrics
        .publish_messages
        .fetch_add(message_ids.len() as u64, Ordering::Relaxed);
    state
        .metrics
        .publish_bytes
        .fetch_add(bytes as u64, Ordering::Relaxed);
    Ok(Json(json!({ "message_ids": message_ids })))
}

async fn native_multi_publish(
    State(state): State<AppState>,
    Query(query): Query<NativeMultiPublishQuery>,
    headers: HeaderMap,
    request: Request<Body>,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, state.publish_token.as_deref(), "publish")?;
    validate_defer(query.defer, &state.config)?;
    validate_native_route(
        &state,
        &query.topic,
        query.partition,
        query.routing_epoch,
        query.topology_generation,
    )
    .await?;
    let (body, _reservation) =
        read_publish_body(&state, request, state.config.limits.max_body_bytes).await?;
    let messages = if query.binary {
        parse_binary_mpub(body, state.config.queue.max_message_bytes)?
    } else {
        parse_text_mpub(body, state.config.queue.max_message_bytes)?
    };
    if messages.is_empty() {
        return Err(ApiError::bad_request(
            "E_BAD_BODY",
            "batch contains no messages",
        ));
    }
    let bytes: usize = messages.iter().map(Bytes::len).sum();
    let message_ids = publish_write(
        &state,
        &query.topic,
        messages,
        query.defer,
        Some(query.partition),
        None,
    )
    .await?;
    state
        .metrics
        .publish_messages
        .fetch_add(message_ids.len() as u64, Ordering::Relaxed);
    state
        .metrics
        .publish_bytes
        .fetch_add(bytes as u64, Ordering::Relaxed);
    Ok(Json(json!({ "message_ids": message_ids })))
}

async fn catalog(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let consensus = state.consensus.as_ref().ok_or_else(cluster_disabled)?;
    let catalog = consensus
        .catalog_snapshot_fresh()
        .await
        .map_err(cluster_error)?;
    let root = consensus
        .root_snapshot_fresh()
        .await
        .map_err(cluster_error)?;
    Ok(Json(json!({
        "shard_id": catalog.shard_id,
        "epoch": catalog.epoch,
        "topics": catalog.topics,
        "root_catalog_shards": root.catalog_shards,
    })))
}

async fn operations(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let consensus = state.consensus.as_ref().ok_or_else(cluster_disabled)?;
    let catalog = consensus
        .catalog_snapshot_fresh()
        .await
        .map_err(cluster_error)?;
    let root = consensus
        .root_snapshot_fresh()
        .await
        .map_err(cluster_error)?;
    Ok(Json(json!({
        "partition_migrations": catalog.migrations,
        "catalog_splits": root.catalog_splits,
        "cell_operations": consensus.metadata().snapshot().operations,
    })))
}

async fn start_migration(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<StartMigrationRequest>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    authorize(&headers, state.admin_token.as_deref(), "admin")?;
    let consensus = state.consensus.as_ref().ok_or_else(cluster_disabled)?;
    let route = consensus
        .catalog_route(&request.topic, 0, Some(request.partition), None)
        .await
        .map_err(route_api_error)?;
    let target = CellId(request.target_cell_id);
    if target == route.partition.home_cell {
        return Err(ApiError::bad_request(
            "E_INVALID",
            "partition already belongs to the requested Home Cell",
        ));
    }
    let response = consensus
        .write(QueueCommand::BeginPartitionMigration {
            topic: request.topic,
            partition: route.partition.id,
            target,
            now_ms: now_ms(),
            max_home_cells: state.config.cluster.federation.max_home_cells_per_topic,
        })
        .await
        .map_err(cluster_error)?;
    let operation_id = *response.message_ids.first().ok_or_else(|| {
        ApiError::internal("E_MIGRATION_FAILED", "Catalog returned no operation ID")
    })?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "operation_id": operation_id })),
    ))
}

async fn resume_migration(
    State(state): State<AppState>,
    Path(operation_id): Path<u64>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, state.admin_token.as_deref(), "admin")?;
    let consensus = state.consensus.as_ref().ok_or_else(cluster_disabled)?;
    let operation = consensus
        .catalog_snapshot_fresh()
        .await
        .map_err(cluster_error)?
        .migrations
        .get(&operation_id)
        .cloned()
        .ok_or_else(|| ApiError::not_found("E_NOT_FOUND", "migration operation not found"))?;
    if operation.phase != PartitionMigrationPhase::NeedsOperator {
        return Err(ApiError::conflict(
            "E_OPERATION_STATE",
            "only a NEEDS_OPERATOR migration can be resumed",
        ));
    }
    consensus
        .write(QueueCommand::AdvancePartitionMigration {
            operation_id,
            expected: PartitionMigrationPhase::NeedsOperator,
            next: PartitionMigrationPhase::Planned,
            observed_lag_entries: operation.observed_lag_entries,
            now_ms: now_ms(),
            max_home_cells: state.config.cluster.federation.max_home_cells_per_topic,
        })
        .await
        .map_err(cluster_error)?;
    Ok(Json(
        json!({ "operation_id": operation_id, "resumed": true }),
    ))
}

async fn describe(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let federation = &state.config.cluster.federation;
    let root = match &state.consensus {
        Some(runtime) if federation.enabled => {
            Some(runtime.root_snapshot_fresh().await.map_err(cluster_error)?)
        }
        _ => None,
    };
    let configured_cells: BTreeMap<_, Vec<_>> = state
        .config
        .cluster
        .nodes
        .iter()
        .filter_map(|(id, node)| node.cell_id.map(|cell| (cell, id)))
        .fold(BTreeMap::new(), |mut cells, (cell, id)| {
            cells.entry(cell).or_default().push(id);
            cells
        });
    let discovered_cells: BTreeSet<_> = state
        .federation_peers
        .ready(now_ms(), 60_000)
        .into_iter()
        .map(|peer| peer.descriptor.cell_id.0)
        .collect();
    Ok(Json(json!({
        "enabled": federation.enabled,
        "format": 6,
        "implementation_stage": "production_candidate",
        "local_cell_id": state.config.local_cell_id().0,
        "configured_cells": configured_cells,
        "discovered_cells": discovered_cells,
        "root_epoch": root.as_ref().map(|root| root.epoch),
        "root_voters": root.as_ref().map(|root| &root.root_voters),
        "root_mode": "bootstrap_colocated",
        "max_home_cells_per_topic": federation.max_home_cells_per_topic,
        "route_cache_ms": federation.route_cache_ms,
        "cell_policy": {
            "min_nodes": federation.cell_min_nodes,
            "target_nodes": federation.cell_target_nodes,
            "max_nodes": federation.cell_max_nodes,
            "routers_per_cell": federation.routers_per_cell,
        },
        "control_plane": {
            "root_group_id": rustqueue_consensus::ROOT_GROUP_ID.to_string(),
            "catalog_sharding": "range_map",
            "data_plane_scope": "home_cell",
        },
        "capabilities": {
            "cell_scoped_data_plane": true,
            "hierarchical_discovery": true,
            "stable_internal_message_identity": true,
            "independent_root_raft": true,
            "independent_catalog_raft_shards": true,
            "cross_cell_publish_forwarding": true,
            "partition_migration_executor": true,
            "catalog_autosplit_executor": false,
        }
    })))
}

async fn route(
    State(state): State<AppState>,
    Query(query): Query<RouteQuery>,
) -> Result<Json<Value>, ApiError> {
    let consensus = state.consensus.as_ref().ok_or_else(|| ApiError {
        status: StatusCode::CONFLICT,
        code: "E_CLUSTER_DISABLED",
        detail: "federation routing requires cluster mode".into(),
    })?;
    let decision = consensus
        .catalog_route(
            &query.topic,
            query.operation_id.unwrap_or_default(),
            query.partition,
            query.key.as_deref().map(str::as_bytes),
        )
        .await
        .map_err(route_api_error)?;
    let target = decision.partition;
    let gateways = producers(&state, Some(&query.topic)).await;
    let gateway = gateways
        .into_iter()
        .find(|producer| producer.cell_id == Some(target.home_cell.0));
    Ok(Json(json!({
        "topic": query.topic,
        "global_group_id": target.id,
        "partition": target.number,
        "wire_slot": target.wire_slot,
        "home_cell_id": target.home_cell,
        "topology_generation": decision.topology_generation,
        "routing_epoch": decision.routing_epoch,
        "cache_ttl_ms": state.config.cluster.federation.route_cache_ms,
        "gateway": gateway,
    })))
}

async fn stats(State(state): State<AppState>) -> Json<Value> {
    let local_cell = state.config.local_cell_id();
    let local = if let Some(consensus) = &state.consensus {
        let result = consensus.cluster_stats().await;
        json!({
            "complete": result.complete,
            "missing_groups": result.missing_groups,
            "collected_at_ms": result.collected_at_ms,
            "topics": result.stats.topics,
        })
    } else {
        json!({
            "complete": true,
            "missing_groups": [],
            "collected_at_ms": now_ms(),
            "topics": state.broker.stats().topics,
        })
    };
    let configured: BTreeSet<_> = state
        .config
        .cluster
        .nodes
        .values()
        .filter_map(|node| node.cell_id.map(CellId))
        .collect();
    let mut peers: BTreeMap<CellId, _> = BTreeMap::new();
    for peer in state.federation_peers.ready(now_ms(), 60_000) {
        peers
            .entry(peer.descriptor.cell_id)
            .or_insert(peer.descriptor);
    }
    let remote = stream::iter(peers)
        .filter(|(cell, _)| std::future::ready(*cell != local_cell))
        .map(|(cell, peer)| async move {
            let url = format!(
                "http://{}:{}/v1/stats",
                peer.broadcast_address, peer.http_port
            );
            let result = tokio::time::timeout(STATS_TIMEOUT, async {
                reqwest::get(url)
                    .await?
                    .error_for_status()?
                    .json::<Value>()
                    .await
            })
            .await;
            let value = match result {
                Ok(Ok(value)) => Some(value),
                _ => None,
            };
            (cell, value)
        })
        .buffer_unordered(16)
        .collect::<Vec<_>>()
        .await;
    let mut cells = BTreeMap::from([(local_cell, local)]);
    for (cell, value) in remote {
        if let Some(value) = value {
            cells.insert(cell, value);
        }
    }
    let missing_cells: Vec<_> = configured
        .difference(&cells.keys().copied().collect())
        .copied()
        .collect();
    Json(json!({
        "complete": missing_cells.is_empty(),
        "collected_at_ms": now_ms(),
        "staleness_budget_ms": 5_000,
        "missing_cells": missing_cells,
        "cells": cells,
    }))
}

async fn validate_native_route(
    state: &AppState,
    topic_name: &str,
    partition_number: u16,
    routing_epoch: u64,
    topology_generation: u64,
) -> Result<(), ApiError> {
    let consensus = state.consensus.as_ref().ok_or_else(cluster_disabled)?;
    let route = consensus
        .catalog_route(topic_name, 0, Some(partition_number), None)
        .await
        .map_err(route_api_error)?;
    if route.routing_epoch != routing_epoch || route.topology_generation != topology_generation {
        return Err(ApiError {
            status: StatusCode::CONFLICT,
            code: "E_ROUTE_STALE",
            detail: "routing epoch changed; refresh /v1/federation/route".into(),
        });
    }
    if route.partition.home_cell != state.config.local_cell_id() {
        return Err(ApiError {
            status: StatusCode::CONFLICT,
            code: "E_WRONG_HOME_CELL",
            detail: format!(
                "partition belongs to Home Cell {}; refresh the direct route",
                route.partition.home_cell
            ),
        });
    }
    if !consensus.gateway_ready() {
        return Err(home_unavailable(
            route.partition.home_cell,
            state.config.cluster.federation.retry_after_ms,
        ));
    }
    Ok(())
}

pub(super) fn route_api_error(error: RouteError) -> ApiError {
    match error {
        RouteError::TopicNotFound => {
            ApiError::not_found("E_BAD_TOPIC", "topic is not present in Catalog")
        }
        RouteError::TopicDeleting => ApiError {
            status: StatusCode::CONFLICT,
            code: "E_TOPIC_DELETING",
            detail: "topic deletion is in progress".into(),
        },
        RouteError::NoActivePartition | RouteError::PartitionNotActive => partition_not_active(),
        RouteError::MigrationFenced { retry_after_ms }
        | RouteError::CatalogUnavailable { retry_after_ms } => ApiError {
            status: StatusCode::TOO_MANY_REQUESTS,
            code: "E_ROUTE_UNAVAILABLE",
            detail: format!("route is temporarily unavailable; retry after {retry_after_ms} ms"),
        },
        RouteError::HomeCellUnavailable {
            cell_id,
            retry_after_ms,
        } => home_unavailable(cell_id, retry_after_ms),
    }
}

pub(super) fn cluster_error(error: anyhow::Error) -> ApiError {
    ApiError::internal("E_CLUSTER", error.to_string())
}

fn partition_not_active() -> ApiError {
    ApiError {
        status: StatusCode::CONFLICT,
        code: "E_PARTITION_NOT_ACTIVE",
        detail: "partition is preparing, retired, or unknown".into(),
    }
}

fn cluster_disabled() -> ApiError {
    ApiError {
        status: StatusCode::CONFLICT,
        code: "E_CLUSTER_DISABLED",
        detail: "federation control plane requires cluster mode".into(),
    }
}

fn home_unavailable(cell: CellId, retry_after_ms: u64) -> ApiError {
    ApiError {
        status: StatusCode::TOO_MANY_REQUESTS,
        code: "E_HOME_CELL_UNAVAILABLE",
        detail: format!("Home Cell {cell} is unavailable; retry after {retry_after_ms} ms"),
    }
}
