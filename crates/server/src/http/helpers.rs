use super::*;

pub(super) fn producer(config: &Config) -> Producer {
    Producer {
        remote_address: format!(
            "{}:{}",
            config.node.broadcast_address, config.network.advertised_tcp_port
        ),
        hostname: config.node.broadcast_address.clone(),
        broadcast_address: config.node.broadcast_address.clone(),
        tcp_port: config.network.advertised_tcp_port,
        http_port: config.network.advertised_http_port,
        version: env!("CARGO_PKG_VERSION"),
        cell_id: config
            .cluster
            .federation
            .enabled
            .then_some(config.cluster.federation.cell_id),
    }
}

pub(super) async fn producers(state: &AppState, topic: Option<&str>) -> Vec<Producer> {
    if !state.config.cluster.enabled {
        return state
            .accepting
            .load(Ordering::Acquire)
            .then(|| producer(&state.config))
            .into_iter()
            .collect();
    }
    let healthy = match &state.consensus {
        Some(consensus) => consensus.healthy_node_ids().await,
        None => std::collections::BTreeSet::new(),
    };
    let metadata = state
        .consensus
        .as_ref()
        .map(|consensus| consensus.metadata().snapshot());
    let now = now_ms();
    let Some(metadata) = metadata else {
        return Vec::new();
    };
    if !state.config.cluster.federation.enabled {
        return state
            .config
            .cluster
            .nodes
            .iter()
            .filter(|(id, _)| {
                id.parse::<u64>().ok().is_some_and(|node_id| {
                    healthy.contains(&node_id)
                        && (node_id != state.config.node.id
                            || state.accepting.load(Ordering::Acquire))
                        && !metadata.drained_nodes.contains(&node_id)
                        && metadata
                            .maintenance_nodes
                            .get(&node_id)
                            .is_none_or(|lease| lease.expires_at_ms <= now)
                })
            })
            .map(|(_, node)| producer_from_config(node))
            .collect();
    }

    let mut requested_cells: std::collections::BTreeSet<_> =
        if let (Some(name), Some(consensus)) = (topic, state.consensus.as_ref()) {
            consensus
                .catalog_topic_descriptor(name)
                .await
                .ok()
                .flatten()
                .map(|topic| topic.home_cells)
                .unwrap_or_default()
        } else {
            std::collections::BTreeSet::new()
        };
    if requested_cells.is_empty() {
        requested_cells = state
            .config
            .cluster
            .nodes
            .values()
            .filter_map(|node| node.cell_id.map(rustqueue_consensus::CellId))
            .collect();
    }
    // Any ready gateway can transparently forward to a Home Cell. Keeping the
    // local Cell discoverable also covers a topic after its final local
    // partition migrates away.
    requested_cells.insert(state.config.local_cell_id());
    let mut candidates: std::collections::BTreeMap<_, Vec<(u64, bool, Producer)>> =
        std::collections::BTreeMap::new();
    for (node_id, descriptor) in &metadata.nodes {
        if !requested_cells.contains(&descriptor.cell_id)
            || !healthy.contains(node_id)
            || metadata.drained_nodes.contains(node_id)
            || metadata
                .maintenance_nodes
                .get(node_id)
                .is_some_and(|lease| lease.expires_at_ms > now)
        {
            continue;
        }
        candidates.entry(descriptor.cell_id).or_default().push((
            *node_id,
            descriptor.federation_router,
            producer_from_descriptor(descriptor),
        ));
    }
    for peer in state.federation_peers.ready(now, 60_000) {
        if requested_cells.contains(&peer.descriptor.cell_id) {
            candidates
                .entry(peer.descriptor.cell_id)
                .or_default()
                .push((
                    peer.descriptor.id,
                    peer.descriptor.federation_router,
                    producer_from_descriptor(&peer.descriptor),
                ));
        }
    }
    candidates
        .into_values()
        .filter_map(|mut nodes| {
            nodes.sort_by_key(|(id, router, _)| (!*router, *id));
            nodes.into_iter().next().map(|(_, _, producer)| producer)
        })
        .collect()
}

fn producer_from_config(node: &crate::config::ClusterNodeConfig) -> Producer {
    Producer {
        remote_address: format!("{}:{}", node.broadcast_address, node.tcp_port),
        hostname: node.broadcast_address.clone(),
        broadcast_address: node.broadcast_address.clone(),
        tcp_port: node.tcp_port,
        http_port: node.http_port,
        version: env!("CARGO_PKG_VERSION"),
        cell_id: node.cell_id,
    }
}

fn producer_from_descriptor(node: &rustqueue_consensus::NodeDescriptor) -> Producer {
    Producer {
        remote_address: format!("{}:{}", node.broadcast_address, node.tcp_port),
        hostname: node.broadcast_address.clone(),
        broadcast_address: node.broadcast_address.clone(),
        tcp_port: node.tcp_port,
        http_port: node.http_port,
        version: env!("CARGO_PKG_VERSION"),
        cell_id: Some(node.cell_id.0),
    }
}

pub(super) async fn publish_write(
    state: &AppState,
    topic: &str,
    messages: Vec<Bytes>,
    defer_ms: u64,
    partition: Option<u16>,
    routing_key: Option<Vec<u8>>,
) -> Result<Vec<u64>, ApiError> {
    if !state.accepting.load(Ordering::Acquire) {
        return Err(ApiError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "E_DRAINING",
            detail: "node is shutting down and no longer accepts publishes".into(),
        });
    }
    if let Some(consensus) = &state.consensus {
        if let Some(number) = partition {
            if let Some(descriptor) = consensus.metadata().topic(topic) {
                let lifecycle = descriptor
                    .partitions
                    .iter()
                    .find(|partition| partition.number == number)
                    .map(|partition| partition.lifecycle);
                if lifecycle != Some(rustqueue_consensus::PartitionLifecycle::Active) {
                    return Err(ApiError {
                        status: StatusCode::CONFLICT,
                        code: "E_PARTITION_NOT_ACTIVE",
                        detail: "explicit partition is preparing, retired, or unknown".into(),
                    });
                }
            }
        }
        let operation_id = state.operation_ids.fetch_add(1, Ordering::Relaxed);
        let response = write_consensus(
            consensus,
            QueueCommand::Publish {
                operation_id,
                topic: topic.to_owned(),
                bodies: messages,
                timestamp_ns: now_ns(),
                available_at_ms: now_ms().saturating_add(defer_ms.min(i64::MAX as u64) as i64),
                partition,
                routing_key,
            },
        )
        .await?;
        Ok(response.message_ids)
    } else {
        Ok(state.broker.publish(
            topic,
            messages,
            Duration::from_millis(defer_ms),
            partition,
            routing_key.as_deref(),
        )?)
    }
}

pub(super) async fn set_channel_pause(
    state: &AppState,
    query: ChannelQuery,
    paused: bool,
) -> Result<(), ApiError> {
    if let Some(consensus) = &state.consensus {
        write_consensus(
            consensus,
            QueueCommand::PauseChannel {
                topic: query.topic,
                channel: query.channel,
                paused,
            },
        )
        .await?;
    } else {
        state
            .broker
            .set_channel_paused(&query.topic, &query.channel, paused)?;
    }
    Ok(())
}

pub(super) async fn set_topic_pause(
    state: &AppState,
    topic: String,
    paused: bool,
) -> Result<(), ApiError> {
    if let Some(consensus) = &state.consensus {
        write_consensus(consensus, QueueCommand::PauseTopic { topic, paused }).await?;
    } else {
        state.broker.set_topic_paused(&topic, paused)?;
    }
    Ok(())
}

pub(super) async fn write_consensus(
    consensus: &ClusterRuntime,
    command: QueueCommand,
) -> Result<QueueResponse, ApiError> {
    let response = consensus.write(command).await.map_err(|error| ApiError {
        status: StatusCode::SERVICE_UNAVAILABLE,
        code: "E_QUORUM_UNAVAILABLE",
        detail: error.to_string(),
    })?;
    if let Some(error) = response.error.as_ref() {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "E_QUEUE",
            detail: error.clone(),
        });
    }
    Ok(response)
}

pub(super) fn operation_seed(node_id: u64) -> u64 {
    let micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64;
    ((node_id & 0xffff) << 48) | (micros & ((1u64 << 48) - 1))
}

pub(super) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

pub(super) fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(i64::MAX as u128) as i64
}

pub(super) fn filter_stats(
    mut stats: BrokerStats,
    topic: Option<&str>,
    channel: Option<&str>,
) -> BrokerStats {
    if let Some(topic) = topic {
        stats.topics.retain(|candidate| candidate.name == topic);
    }
    if let Some(channel) = channel {
        for topic in &mut stats.topics {
            topic.channels.retain(|candidate| candidate == channel);
            for partition in &mut topic.partitions {
                partition
                    .channels
                    .retain(|candidate| candidate.name == channel);
            }
        }
    }
    stats
}

pub(super) async fn read_publish_body(
    state: &AppState,
    request: Request<Body>,
    maximum: usize,
) -> Result<(Bytes, crate::admission::PublishReservation), ApiError> {
    if state
        .consensus
        .as_ref()
        .is_some_and(|runtime| !runtime.storage_eligible())
    {
        return Err(ApiError::disk_throttled());
    }
    let declared = request
        .headers()
        .get(header::CONTENT_LENGTH)
        .map(|value| {
            value
                .to_str()
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .ok_or_else(|| ApiError::bad_request("E_BAD_BODY", "invalid content-length"))
        })
        .transpose()?;
    if declared.is_some_and(|length| length > maximum) {
        return Err(ApiError::bad_request(
            "E_BAD_BODY",
            "body exceeds configured limit",
        ));
    }
    let reservation = state
        .publish_admission
        .try_reserve(declared.unwrap_or(maximum))
        .ok_or_else(ApiError::throttled)?;
    let body = axum::body::to_bytes(request.into_body(), maximum)
        .await
        .map_err(|_| ApiError::bad_request("E_BAD_BODY", "body exceeds configured limit"))?;
    Ok((body, reservation))
}

pub(super) fn parse_binary_mpub(
    body: Bytes,
    max_message_bytes: usize,
) -> Result<Vec<Bytes>, ApiError> {
    rustqueue_protocol::parse_mpub_bytes(body, max_message_bytes)
        .map_err(|error| ApiError::bad_request(error.code(), error.to_string()))
}

pub(super) fn parse_text_mpub(
    body: Bytes,
    max_message_bytes: usize,
) -> Result<Vec<Bytes>, ApiError> {
    let mut messages = Vec::new();
    let mut start = 0;
    for end in body
        .iter()
        .enumerate()
        .filter_map(|(index, byte)| (*byte == b'\n').then_some(index))
        .chain(std::iter::once(body.len()))
    {
        if end > start {
            if end - start > max_message_bytes {
                return Err(ApiError::bad_request(
                    "E_BAD_MESSAGE",
                    "message exceeds configured limit",
                ));
            }
            messages.push(body.slice(start..end));
            if messages.len() > rustqueue_protocol::MAX_MPUB_MESSAGES {
                return Err(ApiError::bad_request(
                    "E_BAD_BODY",
                    "batch message count exceeds limit",
                ));
            }
        }
        start = end.saturating_add(1);
    }
    Ok(messages)
}

pub(super) fn authorize(
    headers: &HeaderMap,
    expected: Option<&str>,
    scope: &'static str,
) -> Result<(), ApiError> {
    let Some(expected) = expected else {
        return Ok(());
    };
    let authorization = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    let alternate = headers
        .get("x-rustqueue-token")
        .and_then(|value| value.to_str().ok());
    if authorization
        .or(alternate)
        .is_some_and(|provided| constant_time_eq(provided.as_bytes(), expected.as_bytes()))
    {
        return Ok(());
    }
    Err(ApiError {
        status: StatusCode::UNAUTHORIZED,
        code: "E_UNAUTHORIZED",
        detail: format!("{scope} authorization required"),
    })
}

pub(super) fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |difference, (left, right)| difference | (left ^ right))
        == 0
}

pub(super) fn validate_defer(defer: u64, config: &Config) -> Result<(), ApiError> {
    if defer > config.queue.max_defer_ms {
        return Err(ApiError::bad_request(
            "E_BAD_MESSAGE",
            "defer exceeds configured maximum",
        ));
    }
    Ok(())
}
