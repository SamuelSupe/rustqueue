mod compat;
mod helpers;
mod kodo_compat;
mod manage;
mod native;
mod nsq_stats;
mod tokens;

use compat::*;
use helpers::*;
use manage::*;
use native::*;
use nsq_stats::*;
use tokens::TokenSet;

use crate::admission::PublishAdmission;
use crate::config::Config;
use crate::metrics::Metrics;
use crate::subscriptions::SubscriptionRegistry;
use axum::body::{Body, Bytes};
use axum::extract::{Query, State};
use axum::http::{header, HeaderMap, HeaderValue, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use rustqueue_queue::{Broker, BrokerError, BrokerStats};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_http::timeout::RequestBodyTimeoutLayer;
use tracing::info;

pub(crate) use kodo_compat::serve_kodo_compat;

#[derive(Clone)]
struct AppState {
    config: Arc<Config>,
    broker: Arc<Broker>,
    metrics: Arc<Metrics>,
    tokens: TokenSet,
    accepting: Arc<AtomicBool>,
    delivering: Arc<AtomicBool>,
    publish_admission: Arc<PublishAdmission>,
    subscriptions: SubscriptionRegistry,
    started_at: i64,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    code: &'static str,
    detail: String,
}

#[derive(Deserialize)]
struct PublishQuery {
    topic: String,
    #[serde(default)]
    defer: u64,
}

#[derive(Deserialize)]
struct MultiPublishQuery {
    topic: String,
    #[serde(default)]
    binary: bool,
    #[serde(default)]
    defer: u64,
}

#[derive(Deserialize)]
struct TopicQuery {
    topic: String,
}

#[derive(Deserialize)]
struct ChannelQuery {
    topic: String,
    channel: String,
}

#[derive(Deserialize)]
struct StatsQuery {
    format: Option<String>,
    topic: Option<String>,
    channel: Option<String>,
    include_clients: Option<bool>,
}

#[derive(Serialize)]
struct LookupResponse {
    channels: Vec<String>,
    producers: Vec<Producer>,
}

#[derive(Clone, Serialize)]
struct Producer {
    remote_address: String,
    hostname: String,
    broadcast_address: String,
    tcp_port: u16,
    http_port: u16,
    version: &'static str,
}

#[allow(clippy::too_many_arguments)]
pub async fn serve(
    config: Arc<Config>,
    broker: Arc<Broker>,
    metrics: Arc<Metrics>,
    accepting: Arc<AtomicBool>,
    delivering: Arc<AtomicBool>,
    publish_admission: Arc<PublishAdmission>,
    subscriptions: SubscriptionRegistry,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let state = app_state(
        config,
        broker,
        metrics,
        accepting,
        delivering,
        publish_admission,
        subscriptions,
    )?;
    let mut router = Router::new()
        .route("/ping", get(ping))
        .route("/info", get(info_handler))
        .route("/pub", post(publish))
        .route("/mpub", post(multi_publish))
        .route("/stats", get(stats))
        .route("/lookup", get(lookup))
        .route("/topics", get(topics))
        .route("/channels", get(channels))
        .route("/nodes", get(nodes))
        .route("/metrics", get(metrics_handler))
        .route("/topic/create", post(create_topic))
        .route("/topic/delete", post(delete_topic))
        .route("/topic/empty", post(empty_topic))
        .route("/topic/pause", post(pause_topic))
        .route("/topic/unpause", post(unpause_topic))
        .route("/channel/create", post(create_channel))
        .route("/channel/delete", post(delete_channel))
        .route("/channel/empty", post(empty_channel))
        .route("/channel/pause", post(pause_channel))
        .route("/channel/unpause", post(unpause_channel))
        .route("/v1/health", get(health))
        .route("/v1/capabilities", get(capabilities))
        .route("/v1/registry/head", get(registry_head))
        .route("/v1/registry", get(registry))
        .route("/v1/drain", get(drain_status).post(set_drain))
        .route("/v1/stats", get(native_stats))
        .route("/v1/observe", get(observe))
        .route("/v1/observe/head", get(observe_head))
        .route("/v1/storage/scrub", post(scrub))
        .layer(middleware::from_fn(nsq_content_negotiation));
    if state.config.security.console_management_enabled {
        router = router
            .route("/v1/manage/topics/{action}", post(manage_topic))
            .route("/v1/manage/channels/{action}", post(manage_channel))
            .route("/v1/manage/fences/sync", post(sync_fences));
    } else if state.config.security.kodo_cleanup_enabled {
        router = router.route(
            "/v1/manage/channels/delete-if-idle",
            post(delete_idle_channel),
        );
    }
    let timeout = state.config.limits.http_body_timeout_ms;
    let address = state.config.network.http_address;
    let tokens = state.tokens.clone();
    let token_shutdown = shutdown.clone();
    let router = router.with_state(state).layer(RequestBodyTimeoutLayer::new(
        std::time::Duration::from_millis(timeout),
    ));
    let listener = TcpListener::bind(address).await?;
    info!(%address, "HTTP API listening");
    let reloader = tokio::spawn(tokens.reload(token_shutdown));
    let result = axum::serve(listener, router)
        .with_graceful_shutdown(wait_for_shutdown(shutdown))
        .await;
    reloader.abort();
    result?;
    Ok(())
}

async fn wait_for_shutdown(mut shutdown: tokio::sync::watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    while shutdown.changed().await.is_ok() {
        if *shutdown.borrow() {
            return;
        }
    }
}

fn app_state(
    config: Arc<Config>,
    broker: Arc<Broker>,
    metrics: Arc<Metrics>,
    accepting: Arc<AtomicBool>,
    delivering: Arc<AtomicBool>,
    publish_admission: Arc<PublishAdmission>,
    subscriptions: SubscriptionRegistry,
) -> anyhow::Result<AppState> {
    Ok(AppState {
        tokens: TokenSet::from_config(&config)?,
        config: Arc::clone(&config),
        broker,
        metrics,
        accepting,
        delivering,
        publish_admission,
        subscriptions,
        started_at: unix_seconds(),
    })
}

fn unix_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .min(i64::MAX as u64) as i64
}

async fn nsq_content_negotiation(request: Request<Body>, next: Next) -> Response {
    let v1 = request
        .headers()
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("application/vnd.nsq") && v.contains("version=1.0"));
    let mut response = next.run(request).await;
    if v1 {
        response.headers_mut().insert(
            "x-nsq-content-type",
            HeaderValue::from_static("nsq; version=1.0"),
        );
    }
    response
}

impl ApiError {
    fn bad_request(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code,
            detail: detail.into(),
        }
    }
    fn unavailable(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code,
            detail: detail.into(),
        }
    }
    fn throttled(detail: impl Into<String>) -> Self {
        Self {
            status: StatusCode::TOO_MANY_REQUESTS,
            code: "E_THROTTLED",
            detail: detail.into(),
        }
    }
    fn conflict(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code,
            detail: detail.into(),
        }
    }
    fn timeout(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            status: StatusCode::REQUEST_TIMEOUT,
            code,
            detail: detail.into(),
        }
    }
}

impl From<BrokerError> for ApiError {
    fn from(error: BrokerError) -> Self {
        let (status, code) = match error {
            BrokerError::TopicNotFound => (StatusCode::NOT_FOUND, "E_BAD_TOPIC"),
            BrokerError::TopicRetiring => (StatusCode::CONFLICT, "E_TOPIC_RETIRING"),
            BrokerError::TopicTombstoned => (StatusCode::CONFLICT, "E_TOPIC_TOMBSTONED"),
            BrokerError::InvalidTopic => (StatusCode::BAD_REQUEST, "E_BAD_TOPIC"),
            BrokerError::ChannelNotFound => (StatusCode::NOT_FOUND, "E_BAD_CHANNEL"),
            BrokerError::ChannelTombstoned => (StatusCode::CONFLICT, "E_CHANNEL_TOMBSTONED"),
            BrokerError::ChannelNotIdle { .. } => (StatusCode::CONFLICT, "E_CHANNEL_NOT_IDLE"),
            BrokerError::ManagementUnavailable => {
                (StatusCode::SERVICE_UNAVAILABLE, "E_MANAGEMENT_UNAVAILABLE")
            }
            BrokerError::RevisionConflict { .. } => (StatusCode::CONFLICT, "E_REVISION_CONFLICT"),
            BrokerError::OperationConflict => (StatusCode::CONFLICT, "E_OPERATION_CONFLICT"),
            BrokerError::InvalidChannel => (StatusCode::BAD_REQUEST, "E_BAD_CHANNEL"),
            BrokerError::MessageTooLarge | BrokerError::BatchTooLarge => {
                (StatusCode::BAD_REQUEST, "E_BAD_MESSAGE")
            }
            BrokerError::TopicLimit
            | BrokerError::PublishWorkerLimit
            | BrokerError::ChannelWorkerLimit
            | BrokerError::ChannelLimit => (StatusCode::TOO_MANY_REQUESTS, "E_THROTTLED"),
            BrokerError::StorageUnavailable
            | BrokerError::Storage(_)
            | BrokerError::Io(_)
            | BrokerError::InvalidRecord(_) => (StatusCode::SERVICE_UNAVAILABLE, "E_STORAGE"),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, "E_INTERNAL"),
        };
        Self {
            status,
            code,
            detail: error.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let retry = self.status == StatusCode::TOO_MANY_REQUESTS;
        let mut response = (
            self.status,
            Json(json!({"message": self.code, "detail": self.detail})),
        )
            .into_response();
        if retry {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustqueue_queue::{ChannelStats, TopicStats};

    #[test]
    fn parses_binary_mpub_strictly() {
        let mut body = 2u32.to_be_bytes().to_vec();
        body.extend_from_slice(&3u32.to_be_bytes());
        body.extend_from_slice(b"one");
        body.extend_from_slice(&3u32.to_be_bytes());
        body.extend_from_slice(b"two");
        assert_eq!(
            parse_binary_mpub(Bytes::from(body.clone()), 10)
                .unwrap()
                .len(),
            2
        );
        body.push(0);
        assert!(parse_binary_mpub(Bytes::from(body), 10).is_err());
    }

    #[test]
    fn durable_record_failures_are_reported_as_storage_unavailable() {
        let error = ApiError::from(BrokerError::InvalidRecord("corrupt local state".into()));
        assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error.code, "E_STORAGE");
    }

    #[test]
    fn registry_exposes_channel_names_instead_of_internal_stats() {
        let topics = registry_topics(&BrokerStats {
            publish_group_commit: Default::default(),
            channel_group_commit: Default::default(),
            latency: Default::default(),
            delivery_budget: Default::default(),
            aggregate: Default::default(),
            topics: vec![TopicStats {
                name: "events".into(),
                paused: false,
                published_count: 3,
                message_count: 3,
                segment_count: 1,
                segment_bytes: 256,
                channels: vec![ChannelStats {
                    name: "workers".into(),
                    depth: 2,
                    message_count: 3,
                    in_flight_count: 1,
                    deferred_count: 0,
                    requeue_count: 0,
                    timeout_count: 0,
                    paused: false,
                    ephemeral: false,
                    ack_cursor: 1,
                    ack_gap: 0,
                }],
            }],
        });
        assert_eq!(topics[0]["channels"], json!(["workers"]));
    }

    #[test]
    fn console_token_is_read_only_and_distinct_from_admin() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer console-secret"),
        );
        let console = tokens::TokenSource::fixed("console", "console-secret");
        let admin = tokens::TokenSource::fixed("admin", "admin-secret");
        let registry = tokens::TokenSource::fixed("registry", "registry-secret");
        assert!(authorize(&headers, &console, "console").is_ok());
        assert!(authorize(&headers, &admin, "admin").is_err());
        assert!(authorize_any(&headers, &[&registry, &console], "read-only").is_ok());
    }

    #[test]
    fn kodo_cleanup_token_is_distinct_from_admin() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer cleanup-secret"),
        );
        let cleanup = tokens::TokenSource::fixed("Kodo cleanup", "cleanup-secret");
        let admin = tokens::TokenSource::fixed("admin", "admin-secret");
        assert!(authorize(&headers, &cleanup, "Kodo cleanup").is_ok());
        assert!(authorize(&headers, &admin, "admin").is_err());
    }
}
