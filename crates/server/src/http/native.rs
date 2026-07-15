use super::*;

pub(super) async fn health(
    State(state): State<AppState>,
    Query(query): Query<HealthQuery>,
) -> Response {
    if !state.accepting.load(Ordering::Acquire) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "not_ready",
                "node_id": state.config.node.id,
                "reason": "shutting_down",
            })),
        )
            .into_response();
    }
    if let Some(consensus) = &state.consensus {
        if !consensus.gateway_ready() {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "status": "not_ready",
                    "node_id": state.config.node.id,
                    "reason": "cluster_or_feature_level_not_ready",
                        "active_feature_level": consensus.active_feature_level(),
                        "supported_feature_level": rustqueue_consensus::CURRENT_FEATURE_LEVEL,
                        "disk_pressure_since_ms": consensus.disk_pressure_since_ms(),
                        "protective_eviction_enabled": consensus.protective_eviction_enabled(),
                })),
            )
                .into_response();
        }
    }
    let disk = state
        .consensus
        .as_ref()
        .and_then(|consensus| consensus.disk_status().ok());
    if disk.is_some_and(|status| !status.eligible) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "not_ready",
                "node_id": state.config.node.id,
                "reason": "disk_pressure",
                "disk": disk,
                "disk_pressure_since_ms": state.consensus.as_ref().and_then(|runtime| runtime.disk_pressure_since_ms()),
                "protective_eviction_enabled": state.consensus.as_ref().is_some_and(|runtime| runtime.protective_eviction_enabled()),
            })),
        )
            .into_response();
    }
    let preparing_operations: Vec<_> = state
        .consensus
        .as_ref()
        .map(|consensus| {
            consensus
                .metadata()
                .operations()
                .into_iter()
                .filter(|operation| {
                    matches!(
                        operation.kind,
                        rustqueue_consensus::OperationKind::ExpandPartitions { .. }
                    ) && !matches!(
                        operation.state,
                        rustqueue_consensus::OperationState::Completed
                            | rustqueue_consensus::OperationState::Cancelled
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    let quorum = if let Some(consensus) = &state.consensus {
        let result = if query.deep {
            consensus.ensure_quorum().await
        } else {
            consensus.ensure_quorum_cached().await
        };
        match result {
            Ok(()) => "healthy",
            Err(error) => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({
                        "status": "not_ready",
                        "node_id": state.config.node.id,
                        "storage": "healthy",
                        "quorum": "unavailable",
                        "disk": disk,
                        "preparing_operations": preparing_operations,
                        "error": error.to_string(),
                    })),
                )
                    .into_response();
            }
        }
    } else {
        "single-node"
    };
    let clock = state.consensus.as_ref().map(|node| node.clock_status());
    Json(json!({
        "status": "ready",
        "node_id": state.config.node.id,
        "storage": "healthy",
        "quorum": quorum,
        "clock": clock,
        "disk": disk,
        "preparing_operations": preparing_operations,
        "active_feature_level": state.consensus.as_ref().map(|runtime| runtime.active_feature_level()),
        "observed_feature_floor": state.consensus.as_ref().map(|runtime| runtime.observed_feature_floor()),
        "disk_pressure_since_ms": state.consensus.as_ref().and_then(|runtime| runtime.disk_pressure_since_ms()),
        "protective_eviction_enabled": state.consensus.as_ref().is_some_and(|runtime| runtime.protective_eviction_enabled()),
    }))
    .into_response()
}

pub(super) async fn cluster(State(state): State<AppState>) -> Json<Value> {
    if let Some(consensus) = &state.consensus {
        let metrics = consensus.raft().metrics().borrow().clone();
        let metadata = consensus.metadata().snapshot();
        let clock = consensus.clock_status();
        return Json(json!({
            "cluster_id": state.config.cluster.name,
            "mode": "raft",
            "node_id": state.config.node.id,
            "state": format!("{:?}", metrics.state),
            "current_leader": metrics.current_leader,
            "term": metrics.current_term,
            "last_log_index": metrics.last_log_index,
            "last_applied": metrics.last_applied,
            "membership": metrics.membership_config,
            "metadata_epoch": metadata.epoch,
            "cell_id": metadata.cell_id,
            "federation_root_epoch": metadata.federation_root.epoch,
            "catalog_shard_id": metadata.catalog.shard_id,
            "catalog_epoch": metadata.catalog.epoch,
            "topic_count": metadata.topics.len(),
            "drained_nodes": metadata.drained_nodes,
            "local_groups": consensus.local_group_ids().await,
            "active_feature_level": metadata.active_feature_level,
            "supported_feature_level": rustqueue_consensus::CURRENT_FEATURE_LEVEL,
            "observed_feature_floor": consensus.observed_feature_floor(),
            "clock": clock,
        }));
    }
    Json(json!({
        "cluster_id": format!("single-{}", state.config.node.id),
        "mode": "single-node",
        "nodes": [{
            "id": state.config.node.id,
            "address": state.config.node.broadcast_address,
            "role": "leader",
            "healthy": true,
        }]
    }))
}

pub(super) async fn partitions(
    State(state): State<AppState>,
    Query(query): Query<PartitionQuery>,
) -> Result<Json<Value>, ApiError> {
    let consensus = state.consensus.as_ref().ok_or_else(|| ApiError {
        status: StatusCode::CONFLICT,
        code: "E_CLUSTER_DISABLED",
        detail: "partition metadata requires Raft mode".into(),
    })?;
    let metadata = consensus.metadata().snapshot();
    let catalog = metadata.catalog.clone();
    let partitions: Vec<_> = metadata
        .topics
        .into_values()
        .filter(|topic| query.topic.as_ref().is_none_or(|name| name == &topic.name))
        .flat_map(|topic| {
            let catalog_topic = catalog.topics.get(&topic.name).cloned();
            let name = topic.name;
            let topology_generation = topic.topology_generation;
            let key_slots = topic.key_routing_slots;
            topic.partitions.into_iter().map(move |partition| {
                let global_group_id = partition.global_id();
                let wire_incarnation = catalog_topic
                    .as_ref()
                    .and_then(|topic| topic.partitions.get(&global_group_id))
                    .map(|partition| partition.wire_incarnation);
                json!({
                    "topic": name,
                    "group_id": partition.group_id,
                    "global_group_id": global_group_id,
                    "group_key": partition.group_key(),
                    "home_cell_id": partition.home_cell,
                    "partition": partition.number,
                    "slot": partition.slot,
                    "replication_factor": partition.replication_factor,
                    "replicas": partition.replicas,
                    "leader_hint": partition.leader_hint,
                    "lifecycle": partition.lifecycle,
                    "operation_id": partition.operation_id,
                    "topology_generation": topology_generation,
                    "key_routing": key_slots.contains(&partition.slot),
                    "wire_incarnation": wire_incarnation,
                })
            })
        })
        .collect();
    Ok(Json(json!({ "partitions": partitions })))
}

pub(super) async fn replicas(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let consensus = state.consensus.as_ref().ok_or_else(|| ApiError {
        status: StatusCode::CONFLICT,
        code: "E_CLUSTER_DISABLED",
        detail: "replica metadata requires Raft mode".into(),
    })?;
    let local: std::collections::BTreeSet<_> =
        consensus.local_group_ids().await.into_iter().collect();
    let local_node_id = state.config.node.id;
    let metadata = consensus.metadata().snapshot();
    let replicas: Vec<_> = metadata
        .topics
        .into_values()
        .flat_map(|topic| {
            let local = &local;
            topic.partitions.into_iter().flat_map(move |partition| {
                let group_key = partition.group_key();
                let group_id = partition.group_id;
                let partition_number = partition.number;
                partition.replicas.into_iter().map(move |node_id| {
                    json!({
                        "group_id": group_id,
                        "group_key": group_key,
                        "partition": partition_number,
                        "node_id": node_id,
                        "status": if node_id == local_node_id && local.contains(&group_key) {
                            "active_local"
                        } else {
                            "assigned"
                        },
                    })
                })
            })
        })
        .collect();
    Ok(Json(json!({ "replicas": replicas })))
}

pub(super) async fn native_stats(
    State(state): State<AppState>,
    Query(query): Query<StatsQuery>,
) -> Response {
    let (complete, missing_groups, collected_at_ms, stats) =
        if let Some(consensus) = &state.consensus {
            let cluster = consensus.cluster_stats().await;
            (
                cluster.complete,
                cluster.missing_groups,
                cluster.collected_at_ms,
                cluster.stats,
            )
        } else {
            (
                true,
                Vec::new(),
                now_ms().max(0) as u64,
                state.broker.stats(),
            )
        };
    let filtered = filter_stats(stats, query.topic.as_deref(), query.channel.as_deref());
    Json(json!({
        "complete": complete,
        "missing_groups": missing_groups,
        "collected_at_ms": collected_at_ms,
        "topics": filtered.topics,
    }))
    .into_response()
}

pub(super) async fn scrub(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, state.admin_token.as_deref(), "admin")?;
    let queue_records = state.broker.scrub()?;
    let cluster = match &state.consensus {
        Some(consensus) => consensus
            .scrub_and_repair()
            .await
            .map_err(|error| ApiError {
                status: StatusCode::SERVICE_UNAVAILABLE,
                code: "E_STORAGE",
                detail: error.to_string(),
            })?,
        None => Default::default(),
    };
    Ok(Json(json!({
        "status": "ok",
        "records_checked": queue_records + cluster.records_checked,
        "replicas_repaired": cluster.replicas_repaired,
    })))
}
