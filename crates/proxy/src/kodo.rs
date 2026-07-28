use crate::backend::{Backend, BackendPool};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const EXPECTED_BROKERS: usize = 3;
const MAX_STATS_BYTES: usize = 16 * 1024 * 1024;
const MAX_STATS_AGGREGATE_BYTES: usize = 32 * 1024 * 1024;
const MAX_STATS_BACKENDS_PER_SHARD: usize = 16;
const MAX_REGISTRY_HEAD_BYTES: usize = 64 * 1024;
const MAX_BACKEND_ERROR_BYTES: usize = 64 * 1024;
pub(crate) const STATS_WORKING_SET_BYTES: usize = 3 * MAX_STATS_AGGREGATE_BYTES;

#[derive(Clone)]
pub(crate) struct KodoConfig {
    pub ordinal: usize,
    pub cleanup_enabled: bool,
    pub cleanup_token: Option<Arc<str>>,
    pub registry_token: Option<Arc<str>>,
}

#[derive(Default, Deserialize, Serialize)]
struct StatsResponse {
    #[serde(default)]
    version: String,
    #[serde(default)]
    health: String,
    #[serde(default)]
    start_time: i64,
    #[serde(default)]
    topics: Vec<TopicStats>,
}

#[derive(Default, Deserialize, Serialize)]
struct TopicStats {
    #[serde(default)]
    topic_name: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    depth: u64,
    #[serde(default)]
    memory_depth: u64,
    #[serde(default)]
    backend_depth: u64,
    #[serde(default)]
    message_count: u64,
    #[serde(default)]
    paused: bool,
    #[serde(default)]
    channels: Vec<ChannelStats>,
}

#[derive(Default, Deserialize, Serialize)]
struct ChannelStats {
    #[serde(default)]
    channel_name: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    depth: u64,
    #[serde(default)]
    memory_depth: u64,
    #[serde(default)]
    backend_depth: u64,
    #[serde(default)]
    message_count: u64,
    #[serde(default)]
    in_flight_count: u64,
    #[serde(default)]
    deferred_count: u64,
    #[serde(default)]
    requeue_count: u64,
    #[serde(default)]
    timeout_count: u64,
    #[serde(default)]
    client_count: usize,
    #[serde(default)]
    clients: Vec<Value>,
    #[serde(default)]
    paused: bool,
}

#[derive(Deserialize)]
struct Registry {
    revision: u64,
    node_id: u64,
}

pub(crate) async fn stats(
    config: &KodoConfig,
    pool: &BackendPool,
    client: &reqwest::Client,
    path_and_query: &str,
) -> Response {
    let Some(backends) = sharded_backends(pool, config.ordinal, MAX_STATS_BACKENDS_PER_SHARD)
    else {
        return unavailable("gateway stats shard has too many brokers");
    };
    if backends.is_empty() {
        return unavailable("gateway stats shard has no broker");
    }

    let mut aggregate = StatsResponse {
        version: env!("CARGO_PKG_VERSION").into(),
        health: "OK".into(),
        start_time: i64::MAX,
        topics: Vec::new(),
    };
    let path = force_json_stats(path_and_query);
    let mut aggregate_bytes = 0usize;
    for backend in backends {
        let response = match client
            .get(format!("{}{path}", backend.http_origin()))
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
        {
            Ok(response) => response,
            Err(error) => {
                tracing::warn!(%error, node_id = backend.node_id, "Kodo stats backend failed");
                return unavailable("broker stats are unavailable");
            }
        };
        let (stats, response_bytes): (StatsResponse, usize) = match read_json_bounded_with_size(
            response,
            MAX_STATS_BYTES,
        )
        .await
        {
            Ok(stats) => stats,
            Err(error) => {
                tracing::warn!(%error, node_id = backend.node_id, "Kodo stats backend was invalid");
                return unavailable("broker stats are invalid");
            }
        };
        aggregate_bytes = match aggregate_bytes.checked_add(response_bytes) {
            Some(total) if total <= MAX_STATS_AGGREGATE_BYTES => total,
            _ => {
                tracing::warn!("Kodo stats aggregate exceeded its response budget");
                return unavailable("broker stats aggregate is too large");
            }
        };
        if stats.start_time > 0 {
            aggregate.start_time = aggregate.start_time.min(stats.start_time);
        }
        merge_topics(&mut aggregate.topics, stats.topics);
    }
    if aggregate.start_time == i64::MAX {
        aggregate.start_time = unix_seconds();
    }
    Json(aggregate).into_response()
}

pub(crate) async fn delete_channel(
    config: &KodoConfig,
    pool: &BackendPool,
    client: &reqwest::Client,
    topic: &str,
    channel: &str,
) -> Response {
    if !config.cleanup_enabled {
        return (
            StatusCode::NOT_FOUND,
            "E_NOT_FOUND Kodo cleanup compatibility is disabled",
        )
            .into_response();
    }
    if topic.is_empty() || channel.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "E_BAD_REQUEST topic and channel are required",
        )
            .into_response();
    }
    let (Some(cleanup_token), Some(registry_token)) = (
        config.cleanup_token.as_deref(),
        config.registry_token.as_deref(),
    ) else {
        return unavailable("Kodo cleanup tokens are unavailable");
    };
    let Some(backend) = complete_sharded_backend(pool, config.ordinal) else {
        return unavailable("complete broker inventory is required for cleanup");
    };

    if let Err(response) = delete_from_backend(
        client,
        cleanup_token,
        registry_token,
        &backend,
        topic,
        channel,
    )
    .await
    {
        return response;
    }
    "OK".into_response()
}

async fn delete_from_backend(
    client: &reqwest::Client,
    cleanup_token: &str,
    registry_token: &str,
    backend: &Backend,
    topic: &str,
    channel: &str,
) -> Result<(), Response> {
    let registry = client
        .get(format!("{}/v1/registry/head", backend.http_origin()))
        .bearer_auth(registry_token)
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|error| {
            tracing::warn!(%error, node_id = backend.node_id, "Kodo cleanup registry read failed");
            unavailable("broker registry is unavailable")
        })?;
    let registry: Registry = read_json_bounded(registry, MAX_REGISTRY_HEAD_BYTES)
        .await
        .map_err(|error| {
            tracing::warn!(%error, node_id = backend.node_id, "Kodo cleanup registry was invalid");
            unavailable("broker registry is invalid")
        })?;
    if registry.node_id != backend.node_id {
        tracing::warn!(
            expected_node_id = backend.node_id,
            actual_node_id = registry.node_id,
            "Kodo cleanup registry identity changed"
        );
        return Err(unavailable("broker registry identity changed"));
    }
    let operation_id = format!(
        "kodo-delete-{:016x}-{:016x}-{:016x}",
        registry.node_id,
        registry.revision,
        stable_hash(topic, channel)
    );
    let response = client
        .post(format!(
            "{}/v1/manage/channels/delete-if-idle",
            backend.http_origin()
        ))
        .bearer_auth(cleanup_token)
        .json(&json!({
            "operation_id": operation_id,
            "topic": topic,
            "channel": channel,
            "expected_revision": registry.revision,
        }))
        .send()
        .await
        .map_err(|error| {
            tracing::warn!(%error, node_id = backend.node_id, "Kodo cleanup request failed");
            unavailable("broker cleanup is unavailable")
        })?;
    if response.status().is_success() {
        return Ok(());
    }
    let status = response.status();
    let detail = read_bytes_bounded(response, MAX_BACKEND_ERROR_BYTES)
        .await
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .unwrap_or_else(|| "broker cleanup failed".into());
    let outward = if status == StatusCode::CONFLICT {
        StatusCode::CONFLICT
    } else if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        StatusCode::BAD_GATEWAY
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    Err((outward, detail).into_response())
}

fn complete_broker_set(pool: &BackendPool) -> Option<Vec<Backend>> {
    let backends = pool.matching_bounded(EXPECTED_BROKERS, |_| true)?;
    (backends.len() == EXPECTED_BROKERS).then_some(backends)
}

fn sharded_backend(pool: &BackendPool, ordinal: usize) -> Option<Backend> {
    sharded_backends(pool, ordinal, 1)?.into_iter().next()
}

fn sharded_backends(pool: &BackendPool, ordinal: usize, maximum: usize) -> Option<Vec<Backend>> {
    if ordinal >= EXPECTED_BROKERS {
        return Some(Vec::new());
    }
    pool.matching_bounded(maximum, |backend| {
        backend.node_id.saturating_sub(1) % EXPECTED_BROKERS as u64 == ordinal as u64
    })
}

fn complete_sharded_backend(pool: &BackendPool, ordinal: usize) -> Option<Backend> {
    complete_broker_set(pool)?;
    sharded_backend(pool, ordinal)
}

fn force_json_stats(path_and_query: &str) -> String {
    let (path, query) = path_and_query
        .split_once('?')
        .unwrap_or((path_and_query, ""));
    let mut parameters: Vec<_> = query
        .split('&')
        .filter(|parameter| {
            !parameter.is_empty()
                && parameter
                    .split_once('=')
                    .map_or(*parameter != "format", |(key, _)| key != "format")
        })
        .collect();
    parameters.push("format=json");
    format!("{path}?{}", parameters.join("&"))
}

fn merge_topics(output: &mut Vec<TopicStats>, topics: Vec<TopicStats>) {
    let mut merged: BTreeMap<String, TopicStats> = output
        .drain(..)
        .map(|topic| (topic.topic_name.clone(), topic))
        .collect();
    for mut topic in topics {
        normalize_topic(&mut topic);
        match merged.get_mut(&topic.topic_name) {
            Some(existing) => merge_topic(existing, topic),
            None => {
                merged.insert(topic.topic_name.clone(), topic);
            }
        }
    }
    *output = merged.into_values().collect();
}

fn normalize_topic(topic: &mut TopicStats) {
    if topic.topic_name.is_empty() {
        topic.topic_name = topic.name.clone();
    }
    topic.name = topic.topic_name.clone();
    for channel in &mut topic.channels {
        if channel.channel_name.is_empty() {
            channel.channel_name = channel.name.clone();
        }
        channel.name = channel.channel_name.clone();
    }
}

fn merge_topic(existing: &mut TopicStats, incoming: TopicStats) {
    existing.depth = existing.depth.saturating_add(incoming.depth);
    existing.memory_depth = existing.memory_depth.saturating_add(incoming.memory_depth);
    existing.backend_depth = existing
        .backend_depth
        .saturating_add(incoming.backend_depth);
    existing.message_count = existing
        .message_count
        .saturating_add(incoming.message_count);
    existing.paused |= incoming.paused;
    let mut channels: BTreeMap<String, ChannelStats> = existing
        .channels
        .drain(..)
        .map(|channel| (channel.channel_name.clone(), channel))
        .collect();
    for channel in incoming.channels {
        match channels.get_mut(&channel.channel_name) {
            Some(existing) => merge_channel(existing, channel),
            None => {
                channels.insert(channel.channel_name.clone(), channel);
            }
        }
    }
    existing.channels = channels.into_values().collect();
}

fn merge_channel(existing: &mut ChannelStats, mut incoming: ChannelStats) {
    existing.depth = existing.depth.saturating_add(incoming.depth);
    existing.memory_depth = existing.memory_depth.saturating_add(incoming.memory_depth);
    existing.backend_depth = existing
        .backend_depth
        .saturating_add(incoming.backend_depth);
    existing.message_count = existing
        .message_count
        .saturating_add(incoming.message_count);
    existing.in_flight_count = existing
        .in_flight_count
        .saturating_add(incoming.in_flight_count);
    existing.deferred_count = existing
        .deferred_count
        .saturating_add(incoming.deferred_count);
    existing.requeue_count = existing
        .requeue_count
        .saturating_add(incoming.requeue_count);
    existing.timeout_count = existing
        .timeout_count
        .saturating_add(incoming.timeout_count);
    existing.client_count = existing.client_count.saturating_add(incoming.client_count);
    existing.clients.append(&mut incoming.clients);
    existing.paused |= incoming.paused;
}

async fn read_json_bounded<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
    maximum: usize,
) -> anyhow::Result<T> {
    read_json_bounded_with_size(response, maximum)
        .await
        .map(|(value, _)| value)
}

async fn read_json_bounded_with_size<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
    maximum: usize,
) -> anyhow::Result<(T, usize)> {
    let bytes = read_bytes_bounded(response, maximum).await?;
    let length = bytes.len();
    Ok((serde_json::from_slice(&bytes)?, length))
}

async fn read_bytes_bounded(
    mut response: reqwest::Response,
    maximum: usize,
) -> anyhow::Result<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|length| length > maximum as u64)
    {
        anyhow::bail!("response body exceeds {maximum} bytes");
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if bytes.len().saturating_add(chunk.len()) > maximum {
            anyhow::bail!("response body exceeds {maximum} bytes");
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn stable_hash(topic: &str, channel: &str) -> u64 {
    topic
        .bytes()
        .chain(std::iter::once(0))
        .chain(channel.bytes())
        .fold(0xcbf29ce484222325, |hash, byte| {
            (hash ^ byte as u64).wrapping_mul(0x100000001b3)
        })
}

fn unix_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .min(i64::MAX as u64) as i64
}

fn unavailable(detail: &'static str) -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, detail).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{header, HeaderMap};
    use axum::routing::{get, post};
    use axum::Router;

    fn bearer_is(headers: &HeaderMap, token: &str) -> bool {
        headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            == Some(token)
    }

    async fn registry_with_registry_token(headers: HeaderMap) -> Response {
        if !bearer_is(&headers, "registry-secret") {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        Json(json!({"revision": 7, "node_id": 1})).into_response()
    }

    async fn delete_with_cleanup_token(headers: HeaderMap) -> Response {
        if !bearer_is(&headers, "cleanup-secret") {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        StatusCode::OK.into_response()
    }

    #[test]
    fn stats_merge_keeps_standard_names_and_counters() {
        let mut topics = Vec::new();
        merge_topics(
            &mut topics,
            vec![TopicStats {
                name: "events".into(),
                message_count: 3,
                channels: vec![ChannelStats {
                    name: "workers".into(),
                    message_count: 3,
                    client_count: 1,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        );
        assert_eq!(topics[0].topic_name, "events");
        assert_eq!(topics[0].channels[0].channel_name, "workers");
        assert_eq!(topics[0].channels[0].client_count, 1);
    }

    #[test]
    fn operation_hash_separates_topic_channel_pairs() {
        assert_ne!(stable_hash("a:b", "c"), stable_hash("a", "b:c"));
    }

    #[test]
    fn stats_requests_always_use_json_without_dropping_filters() {
        assert_eq!(
            force_json_stats("/stats?format=text&topic=events"),
            "/stats?topic=events&format=json"
        );
        assert_eq!(force_json_stats("/stats"), "/stats?format=json");
    }

    #[test]
    fn gateway_ordinal_shards_every_broker_during_scale_transitions() {
        let pool = BackendPool::default();
        pool.replace(
            (1..=3)
                .map(|node_id| Backend {
                    broadcast_address: format!("broker-{node_id}"),
                    tcp_port: 4150,
                    http_port: 4151,
                    node_id,
                })
                .collect(),
        );
        assert_eq!(sharded_backend(&pool, 1).unwrap().node_id, 2);
        pool.replace(pool.all().into_iter().take(2).collect());
        assert_eq!(sharded_backend(&pool, 1).unwrap().node_id, 2);
        assert!(complete_sharded_backend(&pool, 1).is_none());

        pool.replace(
            [1, 4, 7]
                .into_iter()
                .map(|node_id| Backend {
                    broadcast_address: format!("broker-{node_id}"),
                    tcp_port: 4150,
                    http_port: 4151,
                    node_id,
                })
                .collect(),
        );
        assert_eq!(
            sharded_backends(&pool, 0, 3)
                .unwrap()
                .into_iter()
                .map(|backend| backend.node_id)
                .collect::<Vec<_>>(),
            vec![1, 4, 7]
        );
        assert!(sharded_backends(&pool, 0, 2).is_none());
        assert!(sharded_backend(&pool, 0).is_none());
    }

    #[tokio::test]
    async fn cleanup_uses_distinct_registry_and_cleanup_tokens() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/v1/registry/head", get(registry_with_registry_token))
                    .route(
                        "/v1/manage/channels/delete-if-idle",
                        post(delete_with_cleanup_token),
                    ),
            )
            .await
            .unwrap();
        });
        let backend = Backend {
            broadcast_address: address.ip().to_string(),
            tcp_port: 4150,
            http_port: address.port(),
            node_id: 1,
        };

        assert!(delete_from_backend(
            &reqwest::Client::new(),
            "cleanup-secret",
            "registry-secret",
            &backend,
            "events",
            "workers",
        )
        .await
        .is_ok());
        task.abort();
    }

    #[tokio::test]
    async fn backend_error_body_is_bounded() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/error",
                    get(|| async { vec![b'x'; MAX_BACKEND_ERROR_BYTES + 1] }),
                ),
            )
            .await
            .unwrap();
        });
        let response = reqwest::get(format!("http://{address}/error"))
            .await
            .unwrap();

        assert!(read_bytes_bounded(response, MAX_BACKEND_ERROR_BYTES)
            .await
            .is_err());
        task.abort();
    }
}
