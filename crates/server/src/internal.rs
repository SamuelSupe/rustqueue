mod federation;

use crate::config::Config;
use crate::tls;
use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{header::CONTENT_TYPE, StatusCode};
use axum::response::Response;
use axum::routing::{get, post};
use axum::{Json, Router};
use rustqueue_consensus::{
    decode_frame_with_limit, encode_frame_with_limit, wall_time_ms, AppendEntriesRequest,
    ChangeMembershipRequest, ClusterRuntime, CommandEnvelope, EnsureGroupRequest, FetchRequest,
    FetchResponse, GroupKey, InitializeGroupRequest, InstallSnapshotRequest, OperationResponse,
    QueueCommand, QueueResponse, RebalanceGroupRequest, RebalanceStepRequest, ReleaseRequest,
    RepairReplicaRequest, RoutedResponse, TouchRequest, VoteRequest, INTERNAL_APPEND_FRAME_BYTES,
    INTERNAL_BINARY_CONTENT_TYPE, INTERNAL_FETCH_RESPONSE_BYTES, INTERNAL_SMALL_FRAME_BYTES,
    INTERNAL_SNAPSHOT_FRAME_BYTES, INTERNAL_WRITE_FRAME_BYTES, INTERNAL_WRITE_RESPONSE_BYTES,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::info;

pub async fn serve(config: Arc<Config>, runtime: Arc<ClusterRuntime>) -> anyhow::Result<()> {
    let tls_config = config
        .security
        .internal_tls
        .as_ref()
        .expect("cluster configuration validation requires internal TLS");
    let rustls =
        axum_server::tls_rustls::RustlsConfig::from_config(tls::server_config(tls_config)?);
    let small = INTERNAL_SMALL_FRAME_BYTES;
    let router = Router::new()
        .route("/raft/time", get(time))
        .merge(federation::routes())
        .route(
            "/raft/groups/ensure",
            post(ensure_group).layer(DefaultBodyLimit::max(small)),
        )
        .route(
            "/raft/groups/{group}/initialize",
            post(initialize_group).layer(DefaultBodyLimit::max(small)),
        )
        .route("/raft/groups/{group}/learners/{node_id}", post(add_learner))
        .route(
            "/raft/groups/{group}/append",
            post(append).layer(DefaultBodyLimit::max(INTERNAL_APPEND_FRAME_BYTES)),
        )
        .route(
            "/raft/groups/{group}/vote",
            post(vote).layer(DefaultBodyLimit::max(small)),
        )
        .route(
            "/raft/groups/{group}/snapshot",
            post(snapshot).layer(DefaultBodyLimit::max(
                config
                    .cluster
                    .snapshot_max_bytes
                    .min(INTERNAL_SNAPSHOT_FRAME_BYTES),
            )),
        )
        .route(
            "/raft/groups/{group}/write",
            post(write).layer(DefaultBodyLimit::max(INTERNAL_WRITE_FRAME_BYTES)),
        )
        .route(
            "/raft/groups/{group}/fetch",
            post(fetch).layer(DefaultBodyLimit::max(small)),
        )
        .route(
            "/raft/groups/{group}/ready",
            post(ready).layer(DefaultBodyLimit::max(small)),
        )
        .route(
            "/raft/groups/{group}/touch",
            post(touch).layer(DefaultBodyLimit::max(small)),
        )
        .route(
            "/raft/groups/{group}/release",
            post(release).layer(DefaultBodyLimit::max(small)),
        )
        .route(
            "/raft/groups/{group}/quorum",
            post(quorum).layer(DefaultBodyLimit::max(small)),
        )
        .route("/raft/groups/{group}/elect", post(elect))
        .route("/raft/groups/{group}/transfer/{node_id}", post(transfer))
        .route(
            "/raft/groups/{group}/rebalance",
            post(rebalance).layer(DefaultBodyLimit::max(small)),
        )
        .route(
            "/raft/groups/{group}/rebalance-step",
            post(rebalance_step).layer(DefaultBodyLimit::max(small)),
        )
        .route(
            "/raft/groups/{group}/membership",
            post(membership).layer(DefaultBodyLimit::max(small)),
        )
        .route(
            "/raft/groups/{group}/repair",
            post(repair).layer(DefaultBodyLimit::max(small)),
        )
        .route("/raft/groups/{group}/reset", post(reset))
        .route("/raft/groups/{group}/retire", post(retire))
        .route("/raft/groups/{group}/purge", post(purge))
        .route("/raft/groups/{group}/health", get(health))
        .route("/raft/groups/{group}/stats", get(group_stats))
        .with_state(runtime);
    info!(address = %config.network.internal_address, "internal multi-group Raft mTLS listener ready");
    axum_server::bind_rustls(config.network.internal_address, rustls)
        .serve(router.into_make_service())
        .await?;
    Ok(())
}

async fn ensure_group(
    State(runtime): State<Arc<ClusterRuntime>>,
    Json(request): Json<EnsureGroupRequest>,
) -> Result<Json<OperationResponse>, StatusCode> {
    runtime
        .ensure_partition_local(request)
        .await
        .map(|_| Json(OperationResponse { error: None }))
        .map_err(|error| {
            tracing::warn!(%error, "partition group ensure failed");
            StatusCode::SERVICE_UNAVAILABLE
        })
}

async fn initialize_group(
    State(runtime): State<Arc<ClusterRuntime>>,
    Path(group): Path<String>,
    Json(request): Json<InitializeGroupRequest>,
) -> Result<Json<OperationResponse>, StatusCode> {
    let group_key = parse_group_key(&group)?;
    let result = match group_key {
        GroupKey::Partition(group) => runtime.initialize_group_local(group, request.voters).await,
        GroupKey::Root | GroupKey::Catalog { .. } => {
            runtime
                .initialize_control_group_local(group_key, request.voters)
                .await
        }
        GroupKey::CellMetadata { .. } => Err(anyhow::anyhow!(
            "Cell metadata is initialized through the bootstrap path"
        )),
    };
    result
        .map(|_| Json(OperationResponse { error: None }))
        .map_err(|error| {
            tracing::warn!(group_key = %group_key, %error, "partition group initialize failed");
            StatusCode::SERVICE_UNAVAILABLE
        })
}

async fn add_learner(
    State(runtime): State<Arc<ClusterRuntime>>,
    Path((group, node_id)): Path<(String, u64)>,
) -> Result<Json<OperationResponse>, StatusCode> {
    let group = runtime
        .group(parse_group_key(&group)?)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(OperationResponse {
        error: group
            .add_learner_local(node_id)
            .await
            .err()
            .map(|error| error.to_string()),
    }))
}

async fn append(
    State(runtime): State<Arc<ClusterRuntime>>,
    Path(group): Path<String>,
    body: Bytes,
) -> Result<Response, StatusCode> {
    let request: AppendEntriesRequest = decode_binary_limited(&body, INTERNAL_APPEND_FRAME_BYTES)?;
    let group = runtime
        .group(parse_group_key(&group)?)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;
    binary_response_limited(
        &group.raft().append_entries(request).await,
        INTERNAL_SMALL_FRAME_BYTES,
    )
}

async fn vote(
    State(runtime): State<Arc<ClusterRuntime>>,
    Path(group): Path<String>,
    body: Bytes,
) -> Result<Response, StatusCode> {
    let request: VoteRequest = decode_binary_limited(&body, INTERNAL_SMALL_FRAME_BYTES)?;
    let group = runtime
        .group(parse_group_key(&group)?)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;
    binary_response_limited(
        &group.raft().vote(request).await,
        INTERNAL_SMALL_FRAME_BYTES,
    )
}

async fn snapshot(
    State(runtime): State<Arc<ClusterRuntime>>,
    Path(group): Path<String>,
    body: Bytes,
) -> Result<Response, StatusCode> {
    let request: InstallSnapshotRequest =
        decode_binary_limited(&body, INTERNAL_SNAPSHOT_FRAME_BYTES)?;
    let group = runtime
        .group(parse_group_key(&group)?)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;
    binary_response_limited(
        &group.raft().install_snapshot(request).await,
        INTERNAL_SMALL_FRAME_BYTES,
    )
}

async fn write(
    State(runtime): State<Arc<ClusterRuntime>>,
    Path(group): Path<String>,
    body: Bytes,
) -> Result<Response, StatusCode> {
    let envelope: CommandEnvelope = decode_binary_limited(&body, INTERNAL_WRITE_FRAME_BYTES)?;
    envelope.validate().map_err(|_| StatusCode::BAD_REQUEST)?;
    let command = envelope.command;
    let group = runtime
        .group(parse_group_key(&group)?)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;
    let response = if let QueueCommand::Publish { topic, .. } = &command {
        match runtime.ensure_write_safe().and_then(|_| {
            runtime
                .metadata()
                .topic_is_active(topic)
                .then_some(())
                .ok_or_else(|| "topic is not active".to_owned())
        }) {
            Ok(()) => group.write_routed_local(command).await,
            Err(error) => {
                let (leader_id, term) = group.leader_state();
                RoutedResponse::<QueueResponse>::failed(error, leader_id, term)
            }
        }
    } else {
        group.write_routed_local(command).await
    };
    binary_response_limited(&response, INTERNAL_WRITE_RESPONSE_BYTES)
}

async fn fetch(
    State(runtime): State<Arc<ClusterRuntime>>,
    Path(group): Path<String>,
    body: Bytes,
) -> Result<Response, StatusCode> {
    let request: FetchRequest = decode_binary_limited(&body, INTERNAL_SMALL_FRAME_BYTES)?;
    let group = runtime
        .group(parse_group_key(&group)?)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;
    let response = match runtime.ensure_clock_safe() {
        Ok(()) => group.fetch_routed_local(request).await,
        Err(error) => {
            let (leader_id, term) = group.leader_state();
            RoutedResponse::<FetchResponse>::failed(error, leader_id, term)
        }
    };
    binary_response_limited(&response, INTERNAL_FETCH_RESPONSE_BYTES)
}

async fn ready(
    State(runtime): State<Arc<ClusterRuntime>>,
    Path(group): Path<String>,
    body: Bytes,
) -> Result<Response, StatusCode> {
    let request: FetchRequest = decode_binary_limited(&body, INTERNAL_SMALL_FRAME_BYTES)?;
    let group = runtime
        .group(parse_group_key(&group)?)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;
    binary_response_limited(
        &group.ready_routed_local(request).await,
        INTERNAL_SMALL_FRAME_BYTES,
    )
}

async fn release(
    State(runtime): State<Arc<ClusterRuntime>>,
    Path(group): Path<String>,
    body: Bytes,
) -> Result<Response, StatusCode> {
    let request: ReleaseRequest = decode_binary_limited(&body, INTERNAL_SMALL_FRAME_BYTES)?;
    let group = runtime
        .group(parse_group_key(&group)?)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;
    binary_response_limited(
        &group.release_routed_local(request),
        INTERNAL_SMALL_FRAME_BYTES,
    )
}

async fn quorum(
    State(runtime): State<Arc<ClusterRuntime>>,
    Path(group): Path<String>,
    body: Bytes,
) -> Result<Response, StatusCode> {
    let _: () = decode_binary_limited(&body, INTERNAL_SMALL_FRAME_BYTES)?;
    let group = runtime
        .group(parse_group_key(&group)?)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;
    binary_response_limited(
        &group.quorum_routed_local().await,
        INTERNAL_SMALL_FRAME_BYTES,
    )
}

async fn elect(
    State(runtime): State<Arc<ClusterRuntime>>,
    Path(group): Path<String>,
) -> Result<Json<OperationResponse>, StatusCode> {
    let group = runtime
        .group(parse_group_key(&group)?)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(OperationResponse {
        error: group
            .raft()
            .trigger()
            .elect()
            .await
            .err()
            .map(|error| error.to_string()),
    }))
}

async fn transfer(
    State(runtime): State<Arc<ClusterRuntime>>,
    Path((group, node_id)): Path<(String, u64)>,
) -> Result<Json<OperationResponse>, StatusCode> {
    let group = runtime
        .group(parse_group_key(&group)?)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(OperationResponse {
        error: group
            .transfer_leadership(node_id)
            .await
            .err()
            .map(|error| error.to_string()),
    }))
}

async fn rebalance(
    State(runtime): State<Arc<ClusterRuntime>>,
    Path(group): Path<String>,
    body: Bytes,
) -> Result<Response, StatusCode> {
    let request: RebalanceGroupRequest = decode_binary(&body)?;
    let group_id = partition_global_id(parse_group_key(&group)?)?;
    binary_response(&OperationResponse {
        error: runtime
            .rebalance_group_local(group_id, request.voters)
            .await
            .err()
            .map(|error| error.to_string()),
    })
}

async fn rebalance_step(
    State(runtime): State<Arc<ClusterRuntime>>,
    Path(group): Path<String>,
    body: Bytes,
) -> Result<Response, StatusCode> {
    let request: RebalanceStepRequest = decode_binary(&body)?;
    let group_id = partition_global_id(parse_group_key(&group)?)?;
    binary_response(&OperationResponse {
        error: runtime
            .apply_rebalance_step_local(group_id, request.voters, request.phase)
            .await
            .err()
            .map(|error| error.to_string()),
    })
}

async fn membership(
    State(runtime): State<Arc<ClusterRuntime>>,
    Path(group): Path<String>,
    Json(request): Json<ChangeMembershipRequest>,
) -> Result<Json<OperationResponse>, StatusCode> {
    let group = runtime
        .group(parse_group_key(&group)?)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(OperationResponse {
        error: group
            .change_membership_local(request.voters, request.retain_removed_as_learners)
            .await
            .err()
            .map(|error| error.to_string()),
    }))
}

async fn repair(
    State(runtime): State<Arc<ClusterRuntime>>,
    Path(group): Path<String>,
    body: Bytes,
) -> Result<Response, StatusCode> {
    let request: RepairReplicaRequest = decode_binary(&body)?;
    let group_id = partition_global_id(parse_group_key(&group)?)?;
    binary_response(&OperationResponse {
        error: runtime
            .repair_replica(group_id, request.node_id)
            .await
            .err()
            .map(|error| error.to_string()),
    })
}

async fn reset(
    State(runtime): State<Arc<ClusterRuntime>>,
    Path(group): Path<String>,
) -> Result<Json<OperationResponse>, StatusCode> {
    let group_key = parse_group_key(&group)?;
    let group_id = partition_global_id(group_key)?;
    Ok(Json(OperationResponse {
        error: runtime
            .reset_replica_local(group_id)
            .await
            .err()
            .map(|error| error.to_string()),
    }))
}

async fn retire(
    State(runtime): State<Arc<ClusterRuntime>>,
    Path(group): Path<String>,
) -> Result<Json<OperationResponse>, StatusCode> {
    let group_key = parse_group_key(&group)?;
    Ok(Json(OperationResponse {
        error: runtime
            .retire_replica_key_local(group_key)
            .await
            .err()
            .map(|error| error.to_string()),
    }))
}

async fn purge(
    State(runtime): State<Arc<ClusterRuntime>>,
    Path(group): Path<String>,
) -> Result<Json<OperationResponse>, StatusCode> {
    let group_key = parse_group_key(&group)?;
    Ok(Json(OperationResponse {
        error: runtime
            .purge_replica_key_local(group_key)
            .await
            .err()
            .map(|error| error.to_string()),
    }))
}

async fn touch(
    State(runtime): State<Arc<ClusterRuntime>>,
    Path(group): Path<String>,
    body: Bytes,
) -> Result<Response, StatusCode> {
    let request: TouchRequest = decode_binary_limited(&body, INTERNAL_SMALL_FRAME_BYTES)?;
    let group = runtime
        .group(parse_group_key(&group)?)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;
    binary_response_limited(
        &group.touch_routed_local(request),
        INTERNAL_SMALL_FRAME_BYTES,
    )
}

fn decode_binary<T: DeserializeOwned>(body: &[u8]) -> Result<T, StatusCode> {
    decode_binary_limited(body, INTERNAL_SMALL_FRAME_BYTES)
}

fn decode_binary_limited<T: DeserializeOwned>(body: &[u8], limit: usize) -> Result<T, StatusCode> {
    decode_frame_with_limit(body, limit).map_err(|error| {
        tracing::warn!(%error, "invalid internal binary RPC frame");
        StatusCode::BAD_REQUEST
    })
}

fn binary_response<T: Serialize>(value: &T) -> Result<Response, StatusCode> {
    binary_response_limited(value, INTERNAL_SMALL_FRAME_BYTES)
}

fn binary_response_limited<T: Serialize>(value: &T, limit: usize) -> Result<Response, StatusCode> {
    let body = encode_frame_with_limit(value, limit).map_err(|error| {
        tracing::error!(%error, "failed to encode internal binary RPC response");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Response::builder()
        .header(CONTENT_TYPE, INTERNAL_BINARY_CONTENT_TYPE)
        .body(Body::from(body))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn health(
    State(runtime): State<Arc<ClusterRuntime>>,
    Path(group): Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let group_key = parse_group_key(&group)?;
    let group = runtime
        .group(group_key)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;
    let metrics = group.raft().metrics().borrow().clone();
    let clock = runtime.clock_status();
    Ok(Json(json!({
        "group_key": group_key,
        "state": format!("{:?}", metrics.state),
        "current_leader": metrics.current_leader,
        "current_term": metrics.current_term,
        "last_log_index": metrics.last_log_index,
        "last_applied": metrics.last_applied,
        "last_applied_index": metrics.last_applied.map(|log_id| log_id.index),
        "clock": clock,
    })))
}

async fn group_stats(
    State(runtime): State<Arc<ClusterRuntime>>,
    Path(group): Path<String>,
) -> Result<Json<rustqueue_consensus::GroupStatsResponse>, StatusCode> {
    let group_id = partition_global_id(parse_group_key(&group)?)?;
    runtime
        .local_group_stats(group_id)
        .await
        .map(Json)
        .map_err(|_| StatusCode::CONFLICT)
}

fn parse_group_key(value: &str) -> Result<GroupKey, StatusCode> {
    value.parse().map_err(|error| {
        tracing::warn!(group = value, %error, "invalid internal Raft group key");
        StatusCode::BAD_REQUEST
    })
}

fn partition_global_id(group: GroupKey) -> Result<rustqueue_consensus::GlobalGroupId, StatusCode> {
    group.partition_id().ok_or(StatusCode::CONFLICT)
}

async fn time(State(runtime): State<Arc<ClusterRuntime>>) -> Json<Value> {
    let disk = runtime.disk_status().ok();
    Json(json!({
        "node_id": runtime.node_id(),
        "wall_time_ms": wall_time_ms(),
        "clock_healthy": runtime.clock_status().healthy,
        "gateway_ready": runtime.gateway_ready(),
        "version": env!("CARGO_PKG_VERSION"),
        "data_format": rustqueue_storage::DATA_FORMAT_VERSION,
        "command_schema": rustqueue_consensus::COMMAND_SCHEMA_VERSION,
        "rpc_format": rustqueue_consensus::INTERNAL_RPC_FORMAT,
        "rpc_version": rustqueue_consensus::INTERNAL_RPC_VERSION,
        "min_feature_level": rustqueue_consensus::FEATURE_LEVEL_BASELINE,
        "max_feature_level": rustqueue_consensus::CURRENT_FEATURE_LEVEL,
        "active_feature_level": runtime.active_feature_level(),
        "observed_feature_floor": runtime.observed_feature_floor(),
        "disk": disk,
    }))
}
