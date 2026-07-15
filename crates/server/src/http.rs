mod admin;
mod compat;
mod federation;
mod helpers;
mod native;
mod operations;

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
use rustqueue_consensus::{
    ChannelLifecycle, ClusterRuntime, QueueCommand, QueueResponse, TopicState,
};
use rustqueue_queue::{Broker, BrokerError, BrokerStats};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tracing::info;

#[derive(Clone)]
struct AppState {
    config: Arc<Config>,
    broker: Arc<Broker>,
    metrics: Arc<Metrics>,
    admin_token: Option<Arc<str>>,
    publish_token: Option<Arc<str>>,
    consensus: Option<Arc<ClusterRuntime>>,
    operation_ids: Arc<AtomicU64>,
    accepting: Arc<AtomicBool>,
    publish_admission: Arc<PublishAdmission>,
    federation_peers: Arc<crate::discovery::Directory>,
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
    partition: Option<u16>,
    key: Option<String>,
}

#[derive(Deserialize)]
struct MultiPublishQuery {
    topic: String,
    #[serde(default)]
    binary: bool,
    #[serde(default)]
    defer: u64,
    partition: Option<u16>,
    key: Option<String>,
}

#[derive(Deserialize)]
struct TopicQuery {
    topic: String,
    partitions: Option<u16>,
    replication_factor: Option<u8>,
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

#[derive(Default, Deserialize)]
struct HealthQuery {
    #[serde(default)]
    deep: bool,
}

#[derive(Deserialize)]
struct PartitionQuery {
    topic: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    cell_id: Option<u64>,
}

pub async fn serve(
    config: Arc<Config>,
    broker: Arc<Broker>,
    metrics: Arc<Metrics>,
    consensus: Option<Arc<ClusterRuntime>>,
    accepting: Arc<AtomicBool>,
    publish_admission: Arc<PublishAdmission>,
    federation_peers: Arc<crate::discovery::Directory>,
) -> anyhow::Result<()> {
    let state = AppState {
        admin_token: config.read_admin_token()?.map(Arc::from),
        publish_token: config.read_publish_token()?.map(Arc::from),
        config: Arc::clone(&config),
        broker,
        metrics,
        consensus,
        operation_ids: Arc::new(AtomicU64::new(operation_seed(config.node.id))),
        accepting,
        publish_admission,
        federation_peers,
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
        .route("/v1/cluster", get(cluster))
        .route("/v1/partitions", get(partitions))
        .route("/v1/replicas", get(replicas))
        .route("/v1/stats", get(native_stats))
        .merge(federation::routes())
        .route("/v1/storage/scrub", post(scrub))
        .merge(admin::routes())
        .merge(operations::routes())
        .layer(middleware::from_fn(nsq_content_negotiation))
        .with_state(state);
    let listener = TcpListener::bind(config.network.http_address).await?;
    info!(address = %config.network.http_address, "HTTP API listening");
    axum::serve(listener, router).await?;
    Ok(())
}

async fn nsq_content_negotiation(request: Request<axum::body::Body>, next: Next) -> Response {
    let v1 = request
        .headers()
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value.contains("application/vnd.nsq") && value.contains("version=1.0")
        });
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

    fn not_found(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code,
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

    fn internal(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code,
            detail: detail.into(),
        }
    }

    fn throttled() -> Self {
        Self {
            status: StatusCode::TOO_MANY_REQUESTS,
            code: "E_THROTTLED",
            detail: "publish byte budget is exhausted; retry later".into(),
        }
    }

    fn disk_throttled() -> Self {
        Self {
            status: StatusCode::TOO_MANY_REQUESTS,
            code: "E_THROTTLED",
            detail: "cluster storage is above the configured watermark; retry later".into(),
        }
    }
}

impl From<BrokerError> for ApiError {
    fn from(error: BrokerError) -> Self {
        let (status, code) = match error {
            BrokerError::TopicNotFound => (StatusCode::NOT_FOUND, "E_BAD_TOPIC"),
            BrokerError::InvalidTopic => (StatusCode::BAD_REQUEST, "E_BAD_TOPIC"),
            BrokerError::ChannelNotFound => (StatusCode::NOT_FOUND, "E_BAD_CHANNEL"),
            BrokerError::InvalidChannel => (StatusCode::BAD_REQUEST, "E_BAD_CHANNEL"),
            BrokerError::MessageTooLarge | BrokerError::BatchTooLarge => {
                (StatusCode::BAD_REQUEST, "E_BAD_MESSAGE")
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
        let throttled = self.status == StatusCode::TOO_MANY_REQUESTS;
        let mut response = (
            self.status,
            Json(json!({ "message": self.code, "detail": self.detail })),
        )
            .into_response();
        if throttled {
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

    #[test]
    fn parses_binary_mpub_strictly() {
        let mut body = 2u32.to_be_bytes().to_vec();
        body.extend_from_slice(&3u32.to_be_bytes());
        body.extend_from_slice(b"one");
        body.extend_from_slice(&3u32.to_be_bytes());
        body.extend_from_slice(b"two");
        assert_eq!(
            parse_binary_mpub(Bytes::from(body.clone()), 10).unwrap(),
            [Bytes::from_static(b"one"), Bytes::from_static(b"two")]
        );
        body.push(0);
        assert!(parse_binary_mpub(Bytes::from(body), 10).is_err());
    }
}
