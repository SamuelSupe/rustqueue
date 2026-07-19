use super::*;
use std::time::Duration;

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
    }
}

pub(super) async fn publish_write(
    state: &AppState,
    topic: &str,
    messages: Vec<Bytes>,
    defer_ms: u64,
    reservation: crate::admission::PublishReservation,
) -> Result<Vec<u64>, ApiError> {
    if !state.accepting.load(Ordering::Acquire) {
        return Err(ApiError::unavailable("E_DRAINING", "broker is draining"));
    }
    if !state.broker.storage_healthy() {
        return Err(ApiError::unavailable(
            "E_STORAGE",
            "local storage is isolated; broker restart is required",
        ));
    }
    if !state.publish_admission.storage_ready() {
        return Err(ApiError::throttled(
            "local disk is above its publish watermark",
        ));
    }
    Ok(state
        .broker
        .publish_guarded(
            topic,
            messages,
            Duration::from_millis(defer_ms),
            reservation,
        )
        .await?)
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
            topic.channels.retain(|candidate| candidate.name == channel);
        }
    }
    stats
}

pub(super) async fn read_publish_body(
    state: &AppState,
    request: Request<Body>,
    maximum: usize,
    shape: crate::admission::PublishShape,
) -> Result<(Bytes, crate::admission::PublishReservation), ApiError> {
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
        .try_reserve_publish(declared.unwrap_or(maximum), shape)
        .ok_or_else(|| ApiError::throttled("publish byte budget is exhausted; retry later"))?;
    let body = tokio::time::timeout(
        Duration::from_millis(state.config.limits.http_body_timeout_ms),
        axum::body::to_bytes(request.into_body(), maximum),
    )
    .await
    .map_err(|_| ApiError::timeout("E_BODY_TIMEOUT", "request body read timed out"))?
    .map_err(|_| ApiError::bad_request("E_BAD_BODY", "body exceeds configured limit"))?;
    Ok((body, reservation))
}

pub(super) fn parse_binary_mpub(body: Bytes, max: usize) -> Result<Vec<Bytes>, ApiError> {
    rustqueue_protocol::parse_mpub_bytes(body, max)
        .map_err(|error| ApiError::bad_request(error.code(), error.to_string()))
}

pub(super) fn parse_text_mpub(body: Bytes, max: usize) -> Result<Vec<Bytes>, ApiError> {
    let mut messages = Vec::new();
    let mut start = 0;
    for end in body
        .iter()
        .enumerate()
        .filter_map(|(index, byte)| (*byte == b'\n').then_some(index))
        .chain(std::iter::once(body.len()))
    {
        if end > start {
            if end - start > max {
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
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    let alternate = headers
        .get("x-rustqueue-token")
        .and_then(|v| v.to_str().ok());
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

pub(super) fn authorize_any(
    headers: &HeaderMap,
    expected: &[Option<&str>],
    scope: &'static str,
) -> Result<(), ApiError> {
    let expected: Vec<_> = expected.iter().flatten().copied().collect();
    if expected.is_empty() {
        return Ok(());
    }
    let provided = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .or_else(|| {
            headers
                .get("x-rustqueue-token")
                .and_then(|value| value.to_str().ok())
        });
    if provided.is_some_and(|provided| {
        expected
            .iter()
            .any(|expected| constant_time_eq(provided.as_bytes(), expected.as_bytes()))
    }) {
        return Ok(());
    }
    Err(ApiError {
        status: StatusCode::UNAUTHORIZED,
        code: "E_UNAUTHORIZED",
        detail: format!("{scope} authorization required"),
    })
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .fold(0u8, |diff, (left, right)| diff | (left ^ right))
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

pub(super) fn backlog(stats: &BrokerStats) -> (u64, u64, u64) {
    (
        stats.aggregate.message_count,
        stats.aggregate.channel_depth,
        stats.aggregate.channel_in_flight,
    )
}
