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
        "data_format": 7,
        "mode": "share-nothing",
    }))
}

pub(super) async fn publish(
    State(state): State<AppState>,
    Query(query): Query<PublishQuery>,
    headers: HeaderMap,
    request: Request<Body>,
) -> Result<&'static str, ApiError> {
    authorize(&headers, &state.tokens.publish, "publish")?;
    validate_defer(query.defer, &state.config)?;
    let (body, reservation) = read_publish_body(
        &state,
        request,
        state.config.queue.max_message_bytes,
        crate::admission::PublishShape::Single,
    )
    .await?;
    if body.is_empty() {
        return Err(ApiError::bad_request("E_BAD_MESSAGE", "message is empty"));
    }
    let bytes = body.len();
    let ids = publish_write(&state, &query.topic, vec![body], query.defer, reservation).await?;
    state
        .metrics
        .publish_messages
        .fetch_add(ids.len() as u64, Ordering::Relaxed);
    state
        .metrics
        .publish_bytes
        .fetch_add(bytes as u64, Ordering::Relaxed);
    Ok("OK")
}

pub(super) async fn multi_publish(
    State(state): State<AppState>,
    Query(query): Query<MultiPublishQuery>,
    headers: HeaderMap,
    request: Request<Body>,
) -> Result<&'static str, ApiError> {
    authorize(&headers, &state.tokens.publish, "publish")?;
    validate_defer(query.defer, &state.config)?;
    let (body, reservation) = read_publish_body(
        &state,
        request,
        state.config.limits.max_body_bytes,
        crate::admission::PublishShape::Multi,
    )
    .await?;
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
    let bytes = messages.iter().map(Bytes::len).sum::<usize>();
    let ids = publish_write(&state, &query.topic, messages, query.defer, reservation).await?;
    state
        .metrics
        .publish_messages
        .fetch_add(ids.len() as u64, Ordering::Relaxed);
    state
        .metrics
        .publish_bytes
        .fetch_add(bytes as u64, Ordering::Relaxed);
    Ok("OK")
}

pub(super) async fn lookup(
    State(state): State<AppState>,
    Query(query): Query<TopicQuery>,
) -> Result<Json<LookupResponse>, ApiError> {
    let channels = state.broker.channel_names(&query.topic)?;
    let producers = state
        .accepting
        .load(Ordering::Acquire)
        .then(|| producer(&state.config))
        .into_iter()
        .collect();
    Ok(Json(LookupResponse {
        channels,
        producers,
    }))
}

pub(super) async fn topics(State(state): State<AppState>) -> Json<Value> {
    Json(json!({"topics": state.broker.topic_names()}))
}

pub(super) async fn channels(
    State(state): State<AppState>,
    Query(query): Query<TopicQuery>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(
        json!({"channels": state.broker.channel_names(&query.topic)?}),
    ))
}

pub(super) async fn nodes(State(state): State<AppState>) -> Json<Value> {
    let producers: Vec<_> = state
        .accepting
        .load(Ordering::Acquire)
        .then(|| producer(&state.config))
        .into_iter()
        .collect();
    Json(json!({"producers": producers}))
}

pub(super) async fn metrics_handler(State(state): State<AppState>) -> Response {
    if let Err(error) = state.broker.expire_in_flight().await {
        return ApiError::from(error).into_response();
    }
    let mut output = state.metrics.render();
    let queue_stats = state.broker.metrics_stats(
        state.config.metrics.detailed_queue_metrics,
        state.config.metrics.max_detailed_series,
    );
    output.push_str(&crate::metrics::render_broker(
        &queue_stats,
        &state.config.metrics,
    ));
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        output,
    )
        .into_response()
}

pub(super) async fn create_topic(
    State(state): State<AppState>,
    Query(query): Query<TopicQuery>,
    headers: HeaderMap,
) -> Result<&'static str, ApiError> {
    authorize(&headers, &state.tokens.admin, "admin")?;
    state.broker.create_topic(&query.topic).await?;
    Ok("OK")
}

pub(super) async fn delete_topic(
    State(state): State<AppState>,
    Query(query): Query<TopicQuery>,
    headers: HeaderMap,
) -> Result<&'static str, ApiError> {
    authorize(&headers, &state.tokens.admin, "admin")?;
    state.broker.delete_topic(&query.topic).await?;
    Ok("OK")
}

pub(super) async fn empty_topic(
    State(state): State<AppState>,
    Query(query): Query<TopicQuery>,
    headers: HeaderMap,
) -> Result<&'static str, ApiError> {
    authorize(&headers, &state.tokens.admin, "admin")?;
    state.broker.empty_topic(&query.topic).await?;
    Ok("OK")
}

pub(super) async fn pause_topic(
    State(state): State<AppState>,
    Query(query): Query<TopicQuery>,
    headers: HeaderMap,
) -> Result<&'static str, ApiError> {
    authorize(&headers, &state.tokens.admin, "admin")?;
    state.broker.set_topic_paused(&query.topic, true).await?;
    Ok("OK")
}

pub(super) async fn unpause_topic(
    State(state): State<AppState>,
    Query(query): Query<TopicQuery>,
    headers: HeaderMap,
) -> Result<&'static str, ApiError> {
    authorize(&headers, &state.tokens.admin, "admin")?;
    state.broker.set_topic_paused(&query.topic, false).await?;
    Ok("OK")
}

pub(super) async fn create_channel(
    State(state): State<AppState>,
    Query(query): Query<ChannelQuery>,
    headers: HeaderMap,
) -> Result<&'static str, ApiError> {
    authorize(&headers, &state.tokens.admin, "admin")?;
    state
        .broker
        .create_channel(&query.topic, &query.channel)
        .await?;
    Ok("OK")
}

pub(super) async fn delete_channel(
    State(state): State<AppState>,
    Query(query): Query<ChannelQuery>,
    headers: HeaderMap,
) -> Result<&'static str, ApiError> {
    authorize(&headers, &state.tokens.admin, "admin")?;
    state
        .broker
        .delete_channel(&query.topic, &query.channel)
        .await?;
    Ok("OK")
}

pub(super) async fn empty_channel(
    State(state): State<AppState>,
    Query(query): Query<ChannelQuery>,
    headers: HeaderMap,
) -> Result<&'static str, ApiError> {
    authorize(&headers, &state.tokens.admin, "admin")?;
    state
        .broker
        .empty_channel(&query.topic, &query.channel)
        .await?;
    Ok("OK")
}

pub(super) async fn pause_channel(
    State(state): State<AppState>,
    Query(query): Query<ChannelQuery>,
    headers: HeaderMap,
) -> Result<&'static str, ApiError> {
    authorize(&headers, &state.tokens.admin, "admin")?;
    state
        .broker
        .set_channel_paused(&query.topic, &query.channel, true)
        .await?;
    Ok("OK")
}

pub(super) async fn unpause_channel(
    State(state): State<AppState>,
    Query(query): Query<ChannelQuery>,
    headers: HeaderMap,
) -> Result<&'static str, ApiError> {
    authorize(&headers, &state.tokens.admin, "admin")?;
    state
        .broker
        .set_channel_paused(&query.topic, &query.channel, false)
        .await?;
    Ok("OK")
}
