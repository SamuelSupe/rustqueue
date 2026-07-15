use super::{authorize, ApiError, AppState};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use rustqueue_consensus::{ClusterRuntime, GlobalGroupId, GroupKey, NodeDescriptor, OperationKind};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::sync::Arc;

#[derive(Deserialize)]
struct NodeQuery {
    node_id: u64,
    group_key: Option<String>,
    group_id: Option<u64>,
}

#[derive(Default, Deserialize)]
struct AddNodeQuery {
    node_id: Option<u64>,
    group_key: Option<String>,
    group_id: Option<u64>,
}

#[derive(Deserialize)]
struct MembershipRequest {
    voters: BTreeSet<u64>,
    group_key: Option<String>,
    group_id: Option<u64>,
    #[serde(default = "default_retain")]
    retain_removed_as_learners: bool,
}

pub(super) fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/cluster/node/add", post(add_learner))
        .route("/v1/cluster/node/remove", post(remove_node))
        .route("/v1/cluster/transfer-leader", post(transfer_leader))
        .route("/v1/cluster/rebalance", post(change_membership))
        .route("/v1/cluster/drain", post(drain_node))
        .route("/v1/cluster/snapshot", post(snapshot))
        .route(
            "/v1/replicas/{group_id}/{node_id}/repair",
            post(repair_replica),
        )
}

async fn add_learner(
    State(state): State<AppState>,
    Query(query): Query<AddNodeQuery>,
    headers: HeaderMap,
    descriptor: Option<Json<NodeDescriptor>>,
) -> Result<Json<Value>, ApiError> {
    authorize_admin(&state, &headers)?;
    let cluster = cluster(&state)?;
    let node_id = match descriptor {
        Some(Json(descriptor)) => {
            if query
                .node_id
                .is_some_and(|node_id| node_id != descriptor.id)
            {
                return Err(ApiError::bad_request(
                    "E_NODE",
                    "query and JSON node IDs do not match",
                ));
            }
            cluster
                .validate_node_descriptor(&descriptor)
                .map_err(cluster_error)?;
            descriptor.id
        }
        None => query
            .node_id
            .ok_or_else(|| ApiError::bad_request("E_NODE", "node_id is required"))?,
    };
    match resolve_partition(cluster, query.group_key.as_deref(), query.group_id)? {
        Some(group_id) => cluster
            .add_learner_to(group_id, node_id)
            .await
            .map_err(cluster_error)?,
        None => {
            cluster.add_learner(node_id).await.map_err(cluster_error)?;
            cluster
                .set_node_drained(node_id, false)
                .await
                .map_err(cluster_error)?;
        }
    }
    Ok(Json(
        json!({ "status": "learner_caught_up", "node_id": node_id }),
    ))
}

async fn remove_node(
    State(state): State<AppState>,
    Query(query): Query<NodeQuery>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize_admin(&state, &headers)?;
    let node = cluster(&state)?;
    let group_id = resolve_partition(node, query.group_key.as_deref(), query.group_id)?;
    let mut voters = current_voters(node, group_id).await?;
    if !voters.remove(&query.node_id) {
        return Err(ApiError::bad_request("E_NODE", "node is not a voter"));
    }
    apply_membership_change(node, group_id, voters, false).await?;
    Ok(Json(
        json!({ "status": "removed", "node_id": query.node_id }),
    ))
}

async fn drain_node(
    State(state): State<AppState>,
    Query(query): Query<NodeQuery>,
    headers: HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    authorize_admin(&state, &headers)?;
    let cluster = cluster(&state)?;
    let group_id = resolve_partition(cluster, query.group_key.as_deref(), query.group_id)?;
    let kind = if let Some(group_id) = group_id {
        let voters = cluster
            .plan_group_replica_removal(group_id, query.node_id)
            .map_err(cluster_error)?;
        OperationKind::RebalanceGroup { group_id, voters }
    } else {
        OperationKind::DrainNode {
            node_id: query.node_id,
        }
    };
    let operation = cluster
        .enqueue_operation(kind)
        .await
        .map_err(cluster_error)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "status": "accepted", "operation_id": operation.id })),
    )
        .into_response())
}

async fn repair_replica(
    State(state): State<AppState>,
    Path((group, node_id)): Path<(String, u64)>,
    headers: HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    authorize_admin(&state, &headers)?;
    let cluster = cluster(&state)?;
    let group_id = resolve_partition_path(cluster, &group)?;
    let operation = cluster
        .enqueue_operation(OperationKind::RepairReplica { group_id, node_id })
        .await
        .map_err(cluster_error)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "status": "accepted", "operation_id": operation.id })),
    )
        .into_response())
}

async fn transfer_leader(
    State(state): State<AppState>,
    Query(query): Query<NodeQuery>,
    headers: HeaderMap,
) -> Result<axum::response::Response, ApiError> {
    authorize_admin(&state, &headers)?;
    let cluster = cluster(&state)?;
    let group = resolve_group_key(cluster, query.group_key.as_deref(), query.group_id)?
        .unwrap_or_else(|| cluster.metadata_group().group_key());
    let operation = cluster
        .enqueue_operation(OperationKind::TransferLeader {
            group,
            node_id: query.node_id,
        })
        .await
        .map_err(cluster_error)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "status": "accepted", "operation_id": operation.id })),
    )
        .into_response())
}

async fn change_membership(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<MembershipRequest>,
) -> Result<axum::response::Response, ApiError> {
    authorize_admin(&state, &headers)?;
    let cluster = cluster(&state)?;
    let group_id = resolve_partition(cluster, request.group_key.as_deref(), request.group_id)?;
    if let Some(group_id) = group_id {
        let operation = cluster
            .enqueue_operation(OperationKind::RebalanceGroup {
                group_id,
                voters: request.voters,
            })
            .await
            .map_err(cluster_error)?;
        return Ok((
            StatusCode::ACCEPTED,
            Json(json!({ "status": "accepted", "operation_id": operation.id })),
        )
            .into_response());
    }
    apply_membership_change(
        cluster,
        None,
        request.voters.clone(),
        request.retain_removed_as_learners,
    )
    .await?;
    Ok(Json(json!({ "status": "membership_changed", "voters": request.voters })).into_response())
}

async fn snapshot(
    State(state): State<AppState>,
    Query(query): Query<GroupQuery>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize_admin(&state, &headers)?;
    let cluster = cluster(&state)?;
    match resolve_group_key(cluster, query.group_key.as_deref(), query.group_id)? {
        Some(group_key) => cluster.build_group_snapshot(group_key).await,
        None => cluster.build_snapshot().await,
    }
    .map_err(cluster_error)?;
    Ok(Json(json!({ "status": "snapshot_started" })))
}

fn authorize_admin(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    authorize(headers, state.admin_token.as_deref(), "admin")
}

fn cluster(state: &AppState) -> Result<&Arc<ClusterRuntime>, ApiError> {
    state.consensus.as_ref().ok_or_else(|| ApiError {
        status: StatusCode::CONFLICT,
        code: "E_CLUSTER_DISABLED",
        detail: "cluster administration requires Raft mode".into(),
    })
}

#[derive(Deserialize)]
struct GroupQuery {
    group_key: Option<String>,
    group_id: Option<u64>,
}

async fn current_voters(
    node: &ClusterRuntime,
    group_id: Option<GlobalGroupId>,
) -> Result<BTreeSet<u64>, ApiError> {
    let group = match group_id {
        Some(group_id) => node
            .partition_group(group_id)
            .await
            .ok_or_else(|| ApiError::bad_request("E_GROUP", "group is not hosted on this node"))?,
        None => node.metadata_group(),
    };
    let voters = group
        .raft()
        .metrics()
        .borrow()
        .membership_config
        .voter_ids()
        .collect();
    Ok(voters)
}

async fn apply_membership_change(
    node: &ClusterRuntime,
    group_id: Option<GlobalGroupId>,
    voters: BTreeSet<u64>,
    retain_removed_as_learners: bool,
) -> Result<(), ApiError> {
    match group_id {
        Some(group_id) => {
            if !retain_removed_as_learners {
                return Err(ApiError::bad_request(
                    "E_RETAIN",
                    "partition rebalance retains removed replicas until retirement",
                ));
            }
            node.rebalance_group(group_id, voters).await
        }
        None => {
            node.change_membership(voters, retain_removed_as_learners)
                .await
        }
    }
    .map_err(cluster_error)
}

fn resolve_partition(
    node: &ClusterRuntime,
    group_key: Option<&str>,
    legacy_group_id: Option<u64>,
) -> Result<Option<GlobalGroupId>, ApiError> {
    let Some(group) = resolve_group_key(node, group_key, legacy_group_id)? else {
        return Ok(None);
    };
    group
        .partition_id()
        .map(Some)
        .ok_or_else(|| ApiError::bad_request("E_GROUP", "group is not a partition"))
}

fn resolve_partition_path(node: &ClusterRuntime, value: &str) -> Result<GlobalGroupId, ApiError> {
    if let Ok(group) = value.parse::<GroupKey>() {
        return group
            .partition_id()
            .ok_or_else(|| ApiError::bad_request("E_GROUP", "group is not a partition"));
    }
    let legacy = value
        .parse::<u64>()
        .map_err(|_| ApiError::bad_request("E_GROUP", "invalid partition group key"))?;
    resolve_legacy_partition(node, legacy)
}

fn resolve_group_key(
    node: &ClusterRuntime,
    group_key: Option<&str>,
    legacy_group_id: Option<u64>,
) -> Result<Option<GroupKey>, ApiError> {
    if let Some(value) = group_key {
        return value
            .parse()
            .map(Some)
            .map_err(|_| ApiError::bad_request("E_GROUP", "invalid group_key"));
    }
    if legacy_group_id == Some(0) {
        return Ok(Some(node.metadata_group().group_key()));
    }
    legacy_group_id
        .map(|group_id| resolve_legacy_partition(node, group_id).map(GroupKey::Partition))
        .transpose()
}

fn resolve_legacy_partition(
    node: &ClusterRuntime,
    legacy_group_id: u64,
) -> Result<GlobalGroupId, ApiError> {
    if legacy_group_id == 0 {
        return Err(ApiError::bad_request(
            "E_GROUP",
            "partition group_id must be non-zero",
        ));
    }
    let matches = node
        .metadata()
        .snapshot()
        .topics
        .into_values()
        .flat_map(|topic| topic.partitions)
        .filter(|partition| partition.group_id == legacy_group_id)
        .map(|partition| partition.global_id())
        .collect::<BTreeSet<_>>();
    match matches.len() {
        1 => Ok(*matches.iter().next().expect("one match was counted")),
        0 => Err(ApiError::bad_request(
            "E_GROUP",
            "partition group not found",
        )),
        _ => Err(ApiError::bad_request(
            "E_GROUP_AMBIGUOUS",
            "legacy group_id is ambiguous; use group_key=partition-<cell>-<local>",
        )),
    }
}

fn cluster_error(error: anyhow::Error) -> ApiError {
    ApiError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        code: "E_CLUSTER",
        detail: error.to_string(),
    }
}

const fn default_retain() -> bool {
    true
}
