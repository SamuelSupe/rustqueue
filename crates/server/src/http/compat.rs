use super::*;

pub(super) async fn ping() -> &'static str {
    "OK"
}

pub(super) async fn info_handler(State(state): State<AppState>) -> Json<Value> {
    Json(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "broadcast_address": state.config.node.broadcast_address,
        "hostname": state.config.node.broadcast_address,
        "tcp_port": state.config.network.advertised_tcp_port,
        "http_port": state.config.network.advertised_http_port,
        "node_id": state.config.node.id,
        "feature_level": state.consensus.as_ref().map_or(
            rustqueue_consensus::CURRENT_FEATURE_LEVEL,
            |runtime| runtime.active_feature_level(),
        ),
        "observed_feature_floor": state.consensus.as_ref().map_or(
            rustqueue_consensus::CURRENT_FEATURE_LEVEL,
            |runtime| runtime.observed_feature_floor(),
        ),
    }))
}

pub(super) async fn publish(
    State(state): State<AppState>,
    Query(query): Query<PublishQuery>,
    headers: HeaderMap,
    request: Request<Body>,
) -> Result<&'static str, ApiError> {
    authorize(&headers, state.publish_token.as_deref(), "publish")?;
    validate_defer(query.defer, &state.config)?;
    let (body, _reservation) =
        read_publish_body(&state, request, state.config.queue.max_message_bytes).await?;
    if body.is_empty() {
        return Err(ApiError::bad_request("E_BAD_MESSAGE", "message is empty"));
    }
    let body_len = body.len();
    let ids = publish_write(
        &state,
        &query.topic,
        vec![body],
        query.defer,
        query.partition,
        query.key.map(String::into_bytes),
    )
    .await?;
    state
        .metrics
        .publish_messages
        .fetch_add(ids.len() as u64, Ordering::Relaxed);
    state
        .metrics
        .publish_bytes
        .fetch_add(body_len as u64, Ordering::Relaxed);
    Ok("OK")
}

pub(super) async fn multi_publish(
    State(state): State<AppState>,
    Query(query): Query<MultiPublishQuery>,
    headers: HeaderMap,
    request: Request<Body>,
) -> Result<&'static str, ApiError> {
    authorize(&headers, state.publish_token.as_deref(), "publish")?;
    validate_defer(query.defer, &state.config)?;
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
    let total_bytes: usize = messages.iter().map(Bytes::len).sum();
    let ids = publish_write(
        &state,
        &query.topic,
        messages,
        query.defer,
        query.partition,
        query.key.map(String::into_bytes),
    )
    .await?;
    state
        .metrics
        .publish_messages
        .fetch_add(ids.len() as u64, Ordering::Relaxed);
    state
        .metrics
        .publish_bytes
        .fetch_add(total_bytes as u64, Ordering::Relaxed);
    Ok("OK")
}

pub(super) async fn stats(
    State(state): State<AppState>,
    Query(query): Query<StatsQuery>,
) -> Response {
    let stats = if let Some(consensus) = &state.consensus {
        let cluster = consensus.cluster_stats().await;
        if !cluster.complete {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "message": "E_STATS_UNAVAILABLE",
                    "missing_groups": cluster.missing_groups,
                })),
            )
                .into_response();
        }
        cluster.stats
    } else {
        state.broker.stats()
    };
    let filtered = filter_stats(stats, query.topic.as_deref(), query.channel.as_deref());
    if query.format.as_deref() == Some("json") {
        return Json(json!({ "version": env!("CARGO_PKG_VERSION"), "health": "OK", "topics": filtered.topics })).into_response();
    }
    let mut output = String::new();
    for topic in filtered.topics {
        output.push_str(&format!(
            "[{}] depth={} partitions={}\n",
            topic.name,
            topic.message_count,
            topic.partitions.len()
        ));
        for partition in topic.partitions {
            for channel in partition.channels {
                output.push_str(&format!(
                    "   [{}] depth={} in-flight={} deferred={}\n",
                    channel.name, channel.depth, channel.in_flight_count, channel.deferred_count
                ));
            }
        }
    }
    output.into_response()
}

pub(super) async fn lookup(
    State(state): State<AppState>,
    Query(query): Query<TopicQuery>,
) -> Result<Json<LookupResponse>, ApiError> {
    let channels = if let Some(consensus) = &state.consensus {
        if state.config.cluster.federation.enabled {
            consensus
                .catalog_topic_descriptor(&query.topic)
                .await
                .map_err(super::federation::route_api_error)?
                .map(|topic| {
                    topic
                        .channels
                        .into_values()
                        .filter(|channel| channel.state == ChannelLifecycle::Active)
                        .map(|channel| channel.name)
                        .collect()
                })
                .unwrap_or_default()
        } else {
            consensus.metadata().active_channels(&query.topic)
        }
    } else {
        state.broker.channel_names(&query.topic)?
    };
    Ok(Json(LookupResponse {
        channels,
        producers: producers(&state, Some(&query.topic)).await,
    }))
}

pub(super) async fn topics(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let topics = if let Some(consensus) = &state.consensus {
        if state.config.cluster.federation.enabled {
            consensus
                .catalog_snapshot_fresh()
                .await
                .map_err(super::federation::cluster_error)?
                .topics
                .into_keys()
                .collect::<Vec<_>>()
        } else {
            consensus
                .metadata()
                .snapshot()
                .topics
                .into_values()
                .filter(|topic| topic.state == TopicState::Active)
                .map(|topic| topic.name)
                .collect::<Vec<_>>()
        }
    } else {
        state.broker.topic_names()
    };
    Ok(Json(json!({ "topics": topics })))
}

pub(super) async fn channels(
    State(state): State<AppState>,
    Query(query): Query<TopicQuery>,
) -> Result<Json<Value>, ApiError> {
    let channels = if let Some(consensus) = &state.consensus {
        if state.config.cluster.federation.enabled {
            consensus
                .catalog_topic_descriptor(&query.topic)
                .await
                .map_err(super::federation::route_api_error)?
                .map(|topic| {
                    topic
                        .channels
                        .into_values()
                        .filter(|channel| channel.state == ChannelLifecycle::Active)
                        .map(|channel| channel.name)
                        .collect()
                })
                .unwrap_or_default()
        } else {
            consensus.metadata().active_channels(&query.topic)
        }
    } else {
        state.broker.channel_names(&query.topic)?
    };
    Ok(Json(json!({ "channels": channels })))
}

pub(super) async fn nodes(State(state): State<AppState>) -> Json<Value> {
    Json(json!({ "producers": producers(&state, None).await }))
}

pub(super) async fn metrics_handler(State(state): State<AppState>) -> Response {
    let mut output = state.metrics.render();
    output.push_str(&crate::metrics::render_broker(&state.broker.stats()));
    if let Some(consensus) = &state.consensus {
        output.push_str(&consensus.render_prometheus_metrics().await);
    }
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        output,
    )
        .into_response()
}

pub(super) async fn create_topic(
    State(state): State<AppState>,
    Query(query): Query<TopicQuery>,
    headers: HeaderMap,
) -> Result<&'static str, ApiError> {
    authorize(&headers, state.admin_token.as_deref(), "admin")?;
    if let Some(consensus) = &state.consensus {
        write_consensus(
            consensus,
            QueueCommand::CreateTopic {
                topic: query.topic,
                partitions: query.partitions,
                replication_factor: query.replication_factor,
            },
        )
        .await?;
    } else {
        if query.replication_factor.is_some_and(|value| value != 1) {
            return Err(ApiError::bad_request(
                "E_REPLICATION_FACTOR",
                "standalone topics use replication_factor=1",
            ));
        }
        state.broker.create_topic(&query.topic, query.partitions)?;
    }
    Ok("OK")
}

pub(super) async fn delete_topic(
    State(state): State<AppState>,
    Query(query): Query<TopicQuery>,
    headers: HeaderMap,
) -> Result<&'static str, ApiError> {
    authorize(&headers, state.admin_token.as_deref(), "admin")?;
    if let Some(consensus) = &state.consensus {
        write_consensus(consensus, QueueCommand::DeleteTopic { topic: query.topic }).await?;
    } else {
        state.broker.delete_topic(&query.topic)?;
    }
    Ok("OK")
}

pub(super) async fn empty_topic(
    State(state): State<AppState>,
    Query(query): Query<TopicQuery>,
    headers: HeaderMap,
) -> Result<&'static str, ApiError> {
    authorize(&headers, state.admin_token.as_deref(), "admin")?;
    if let Some(consensus) = &state.consensus {
        write_consensus(consensus, QueueCommand::EmptyTopic { topic: query.topic }).await?;
    } else {
        state.broker.empty_topic(&query.topic)?;
    }
    Ok("OK")
}

pub(super) async fn pause_topic(
    State(state): State<AppState>,
    Query(query): Query<TopicQuery>,
    headers: HeaderMap,
) -> Result<&'static str, ApiError> {
    authorize(&headers, state.admin_token.as_deref(), "admin")?;
    set_topic_pause(&state, query.topic, true).await?;
    Ok("OK")
}

pub(super) async fn unpause_topic(
    State(state): State<AppState>,
    Query(query): Query<TopicQuery>,
    headers: HeaderMap,
) -> Result<&'static str, ApiError> {
    authorize(&headers, state.admin_token.as_deref(), "admin")?;
    set_topic_pause(&state, query.topic, false).await?;
    Ok("OK")
}

pub(super) async fn create_channel(
    State(state): State<AppState>,
    Query(query): Query<ChannelQuery>,
    headers: HeaderMap,
) -> Result<&'static str, ApiError> {
    authorize(&headers, state.admin_token.as_deref(), "admin")?;
    if let Some(consensus) = &state.consensus {
        write_consensus(
            consensus,
            QueueCommand::CreateChannel {
                topic: query.topic,
                channel: query.channel,
            },
        )
        .await?;
    } else {
        state.broker.create_channel(&query.topic, &query.channel)?;
    }
    Ok("OK")
}

pub(super) async fn delete_channel(
    State(state): State<AppState>,
    Query(query): Query<ChannelQuery>,
    headers: HeaderMap,
) -> Result<&'static str, ApiError> {
    authorize(&headers, state.admin_token.as_deref(), "admin")?;
    if let Some(consensus) = &state.consensus {
        write_consensus(
            consensus,
            QueueCommand::DeleteChannel {
                topic: query.topic,
                channel: query.channel,
            },
        )
        .await?;
    } else {
        state.broker.delete_channel(&query.topic, &query.channel)?;
    }
    Ok("OK")
}

pub(super) async fn empty_channel(
    State(state): State<AppState>,
    Query(query): Query<ChannelQuery>,
    headers: HeaderMap,
) -> Result<&'static str, ApiError> {
    authorize(&headers, state.admin_token.as_deref(), "admin")?;
    if let Some(consensus) = &state.consensus {
        write_consensus(
            consensus,
            QueueCommand::EmptyChannel {
                topic: query.topic,
                channel: query.channel,
            },
        )
        .await?;
    } else {
        state.broker.empty_channel(&query.topic, &query.channel)?;
    }
    Ok("OK")
}

pub(super) async fn pause_channel(
    State(state): State<AppState>,
    Query(query): Query<ChannelQuery>,
    headers: HeaderMap,
) -> Result<&'static str, ApiError> {
    authorize(&headers, state.admin_token.as_deref(), "admin")?;
    set_channel_pause(&state, query, true).await?;
    Ok("OK")
}

pub(super) async fn unpause_channel(
    State(state): State<AppState>,
    Query(query): Query<ChannelQuery>,
    headers: HeaderMap,
) -> Result<&'static str, ApiError> {
    authorize(&headers, state.admin_token.as_deref(), "admin")?;
    set_channel_pause(&state, query, false).await?;
    Ok("OK")
}
