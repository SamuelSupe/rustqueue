mod compat;
mod helpers;
mod native;

use compat::*;
use helpers::*;
use native::*;

use crate::admission::PublishAdmission;
use crate::config::Config;
use crate::metrics::Metrics;
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
use tracing::info;

#[derive(Clone)]
struct AppState {
    config: Arc<Config>,
    broker: Arc<Broker>,
    metrics: Arc<Metrics>,
    admin_token: Option<Arc<str>>,
    publish_token: Option<Arc<str>>,
    registry_token: Option<Arc<str>>,
    accepting: Arc<AtomicBool>,
    publish_admission: Arc<PublishAdmission>,
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

pub async fn serve(
    config: Arc<Config>,
    broker: Arc<Broker>,
    metrics: Arc<Metrics>,
    accepting: Arc<AtomicBool>,
    publish_admission: Arc<PublishAdmission>,
) -> anyhow::Result<()> {
    let state = AppState {
        admin_token: config.read_admin_token()?.map(Arc::from),
        publish_token: config.read_publish_token()?.map(Arc::from),
        registry_token: config.read_registry_token()?.map(Arc::from),
        config: Arc::clone(&config),
        broker,
        metrics,
        accepting,
        publish_admission,
    };
    let router = Router::new()
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
        .route("/v1/registry", get(registry))
        .route("/v1/drain", get(drain_status).post(set_drain))
        .route("/v1/stats", get(native_stats))
        .route("/v1/storage/scrub", post(scrub))
        .layer(middleware::from_fn(nsq_content_negotiation))
        .with_state(state);
    let listener = TcpListener::bind(config.network.http_address).await?;
    info!(address = %config.network.http_address, "HTTP API listening");
    axum::serve(listener, router).await?;
    Ok(())
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
}

impl From<BrokerError> for ApiError {
    fn from(error: BrokerError) -> Self {
        let (status, code) = match error {
            BrokerError::TopicNotFound => (StatusCode::NOT_FOUND, "E_BAD_TOPIC"),
            BrokerError::TopicRetiring => (StatusCode::CONFLICT, "E_TOPIC_RETIRING"),
            BrokerError::InvalidTopic => (StatusCode::BAD_REQUEST, "E_BAD_TOPIC"),
            BrokerError::ChannelNotFound => (StatusCode::NOT_FOUND, "E_BAD_CHANNEL"),
            BrokerError::InvalidChannel => (StatusCode::BAD_REQUEST, "E_BAD_CHANNEL"),
            BrokerError::MessageTooLarge | BrokerError::BatchTooLarge => {
                (StatusCode::BAD_REQUEST, "E_BAD_MESSAGE")
            }
            BrokerError::BacklogLimit => (StatusCode::TOO_MANY_REQUESTS, "E_THROTTLED"),
            BrokerError::StorageUnavailable | BrokerError::Storage(_) | BrokerError::Io(_) => {
                (StatusCode::SERVICE_UNAVAILABLE, "E_STORAGE")
            }
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
    fn registry_exposes_channel_names_instead_of_internal_stats() {
        let topics = registry_topics(&BrokerStats {
            publish_group_commit: Default::default(),
            topics: vec![TopicStats {
                name: "events".into(),
                paused: false,
                message_count: 3,
                channels: vec![ChannelStats {
                    name: "workers".into(),
                    depth: 2,
                    in_flight_count: 1,
                    deferred_count: 0,
                    paused: false,
                    ephemeral: false,
                    ack_cursor: 1,
                    ack_gap: 0,
                }],
            }],
        });
        assert_eq!(topics[0]["channels"], json!(["workers"]));
    }
}
