use crate::backend::BackendPool;
use crate::kodo::{self, KodoConfig};
use crate::metrics::ProxyMetrics;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{header, Method, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::{Json, Router};
use rustqueue_proxy::{parse_forward_metadata, ForwardMetadataError};
use serde_json::json;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{watch, Semaphore};

const MAX_BACKEND_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const KODO_STATS_HTTP_PORTS: [u16; 3] = [4151, 4154, 4155];
const KODO_METRICS_HTTP_PORT: u16 = 4160;
const KODO_STATS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Clone)]
struct ProxyState {
    pool: BackendPool,
    broker_pool: BackendPool,
    client: reqwest::Client,
    max_body_bytes: usize,
    body_timeout: std::time::Duration,
    inflight_bytes: Arc<Semaphore>,
    metrics: ProxyMetrics,
    kodo: Option<KodoConfig>,
}

pub struct Limits {
    pub max_body_bytes: usize,
    pub inflight_bytes: Arc<Semaphore>,
    pub body_timeout: std::time::Duration,
}

pub async fn serve(
    address: SocketAddr,
    pool: BackendPool,
    broker_pool: BackendPool,
    limits: Limits,
    metrics: ProxyMetrics,
    kodo: Option<KodoConfig>,
    shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_millis(500))
        .timeout(std::time::Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let state = ProxyState {
        pool,
        broker_pool,
        client,
        max_body_bytes: limits.max_body_bytes,
        body_timeout: limits.body_timeout,
        inflight_bytes: limits.inflight_bytes,
        metrics,
        kodo,
    };
    if state.kodo.is_some() {
        if address.port() != KODO_STATS_HTTP_PORTS[0] {
            anyhow::bail!("Kodo Gateway HTTP address must use port 4151");
        }
        let first = state_for_stats_shard(&state, 0);
        let second = state_for_stats_shard(&state, 1);
        let third = state_for_stats_shard(&state, 2);
        let mut second_address = address;
        second_address.set_port(KODO_STATS_HTTP_PORTS[1]);
        let mut third_address = address;
        third_address.set_port(KODO_STATS_HTTP_PORTS[2]);
        let mut metrics_address = address;
        metrics_address.set_port(KODO_METRICS_HTTP_PORT);
        let metrics = state.clone();
        tokio::try_join!(
            serve_one(address, first, shutdown.clone()),
            serve_one(second_address, second, shutdown.clone()),
            serve_one(third_address, third, shutdown.clone()),
            serve_metrics(metrics_address, metrics, shutdown),
        )?;
        return Ok(());
    }
    serve_one(address, state, shutdown).await
}

fn state_for_stats_shard(state: &ProxyState, ordinal: usize) -> ProxyState {
    let mut state = state.clone();
    state.kodo.as_mut().expect("Kodo state was checked").ordinal = ordinal;
    state
}

async fn serve_one(
    address: SocketAddr,
    state: ProxyState,
    shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let router = Router::new()
        .route("/ping", get(ping))
        .route("/v1/health", get(health))
        .route("/metrics", get(prometheus))
        .route("/stats", get(kodo_stats))
        .route("/channel/delete", axum::routing::post(kodo_delete_channel))
        .fallback(any(forward))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(address).await?;
    tracing::info!(%address, "producer HTTP proxy listening");
    axum::serve(listener, router)
        .with_graceful_shutdown(wait_for_shutdown(shutdown))
        .await?;
    Ok(())
}

async fn serve_metrics(
    address: SocketAddr,
    state: ProxyState,
    shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let router = Router::new()
        .route("/ping", get(ping))
        .route("/metrics", get(prometheus))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(address).await?;
    tracing::info!(%address, "Kodo Gateway metrics listening");
    axum::serve(listener, router)
        .with_graceful_shutdown(wait_for_shutdown(shutdown))
        .await?;
    Ok(())
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    while shutdown.changed().await.is_ok() {
        if *shutdown.borrow() {
            return;
        }
    }
}

async fn kodo_stats(State(state): State<ProxyState>, request: Request<Body>) -> Response {
    let Some(config) = &state.kodo else {
        return forward(State(state), request).await;
    };
    let Ok(permits) = u32::try_from(kodo::STATS_WORKING_SET_BYTES) else {
        return stats_throttled();
    };
    let Ok(_permit) = Arc::clone(&state.inflight_bytes).try_acquire_many_owned(permits) else {
        return stats_throttled();
    };
    match tokio::time::timeout(
        KODO_STATS_TIMEOUT,
        kodo::stats(
            config,
            &state.broker_pool,
            &state.client,
            &request.uri().to_string(),
        ),
    )
    .await
    {
        Ok(response) => response,
        Err(_) => stats_timeout(),
    }
}

#[derive(serde::Deserialize)]
struct ChannelQuery {
    topic: String,
    channel: String,
}

async fn kodo_delete_channel(State(state): State<ProxyState>, request: Request<Body>) -> Response {
    let Some(config) = &state.kodo else {
        return forward(State(state), request).await;
    };
    let Ok(axum::extract::Query(query)) =
        axum::extract::Query::<ChannelQuery>::try_from_uri(request.uri())
    else {
        return (
            StatusCode::BAD_REQUEST,
            "E_BAD_REQUEST topic and channel are required",
        )
            .into_response();
    };
    kodo::delete_channel(
        config,
        &state.broker_pool,
        &state.client,
        &query.topic,
        &query.channel,
    )
    .await
}

async fn prometheus(State(state): State<ProxyState>) -> String {
    let mut output = state.metrics.render();
    output.push_str(
        "# HELP rustqueue_proxy_publish_backends Number of publish-ready Brokers known to the proxy.\n\
# TYPE rustqueue_proxy_publish_backends gauge\n\
# HELP rustqueue_proxy_broker_backends Number of Brokers known to the proxy for compatibility reads.\n\
# TYPE rustqueue_proxy_broker_backends gauge\n\
# HELP rustqueue_proxy_kodo_gateway Whether this process terminates the Kodo producer protocol.\n\
# TYPE rustqueue_proxy_kodo_gateway gauge\n",
    );
    output.push_str(&format!(
        "rustqueue_proxy_publish_backends {}\n\
rustqueue_proxy_broker_backends {}\n\
rustqueue_proxy_kodo_gateway {}\n",
        state.pool.len(),
        state.broker_pool.len(),
        u8::from(state.kodo.is_some()),
    ));
    output
}

async fn ping() -> &'static str {
    "OK"
}

async fn health(State(state): State<ProxyState>) -> Response {
    let count = state.pool.len();
    let status = if count > 0 {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(json!({"status": if count > 0 {"ready"} else {"not_ready"}, "brokers": count})),
    )
        .into_response()
}

async fn forward(State(state): State<ProxyState>, request: Request<Body>) -> Response {
    if state.kodo.is_some() {
        return (
            StatusCode::NOT_FOUND,
            "E_NOT_FOUND Kodo Gateway HTTP forwarding is disabled",
        )
            .into_response();
    }
    let (parts, body) = request.into_parts();
    let content_length = parts
        .headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok());
    let metadata = match parse_forward_metadata(
        &parts.uri.to_string(),
        content_length,
        state.max_body_bytes,
    ) {
        Ok(metadata) => metadata,
        Err(ForwardMetadataError::BodyTooLarge) => return body_too_large(),
        Err(ForwardMetadataError::Invalid) => {
            return (
                StatusCode::BAD_REQUEST,
                "E_BAD_REQUEST invalid proxy request",
            )
                .into_response()
        }
    };
    let declared = metadata.declared_bytes;
    let reserved = declared.unwrap_or(state.max_body_bytes).max(1);
    let Ok(reserved) = u32::try_from(reserved) else {
        return throttled();
    };
    let Ok(_permit) = Arc::clone(&state.inflight_bytes).try_acquire_many_owned(reserved) else {
        return throttled();
    };
    let body = match tokio::time::timeout(
        state.body_timeout,
        axum::body::to_bytes(body, state.max_body_bytes),
    )
    .await
    {
        Ok(Ok(body)) => body,
        Ok(Err(_)) => return body_too_large(),
        Err(_) => return body_timeout(),
    };
    let path = metadata.path_and_query;
    let retry_safe = retry_safe_method(&parts.method);
    let backends = state.pool.shuffled(if retry_safe { 2 } else { 1 });
    if backends.is_empty() {
        return unavailable();
    }
    let mut last_error = None;
    for backend in backends {
        let _backend_timer = state.metrics.backend.timer();
        let url = format!("{}{path}", backend.http_origin());
        let mut outgoing = state
            .client
            .request(parts.method.clone(), url)
            .body(body.clone());
        for (name, value) in &parts.headers {
            if *name != header::HOST && *name != header::CONTENT_LENGTH {
                outgoing = outgoing.header(name, value);
            }
        }
        match outgoing.send().await {
            Ok(response) => {
                return backend_response(response, retry_safe, &state.metrics).await;
            }
            Err(error) => last_error = Some(error),
        }
    }
    if let Some(error) = last_error {
        tracing::debug!(%error, "producer HTTP backend failed");
        if !retry_safe {
            state
                .metrics
                .producer_ambiguous_failures
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return ambiguous();
        }
    }
    unavailable()
}

fn retry_safe_method(method: &Method) -> bool {
    method == Method::GET || method == Method::HEAD || method == Method::OPTIONS
}

async fn backend_response(
    response: reqwest::Response,
    retry_safe: bool,
    metrics: &ProxyMetrics,
) -> Response {
    let status = response.status();
    let headers = response.headers().clone();
    let body = match read_body_bounded(response, MAX_BACKEND_RESPONSE_BYTES).await {
        Some(body) => body,
        None => {
            tracing::debug!("backend response body failed or exceeded its limit");
            if retry_safe {
                return unavailable();
            }
            metrics
                .producer_ambiguous_failures
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return ambiguous();
        }
    };
    let mut output = Response::builder().status(status);
    for (name, value) in &headers {
        if *name != header::CONTENT_LENGTH && *name != header::TRANSFER_ENCODING {
            output = output.header(name, value);
        }
    }
    output
        .body(Body::from(body))
        .unwrap_or_else(|_| unavailable())
}

async fn read_body_bounded(mut response: reqwest::Response, maximum: usize) -> Option<Bytes> {
    if response
        .content_length()
        .is_some_and(|length| length > maximum as u64)
    {
        return None;
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.ok()? {
        if body.len().saturating_add(chunk.len()) > maximum {
            return None;
        }
        body.extend_from_slice(&chunk);
    }
    Some(Bytes::from(body))
}

fn unavailable() -> Response {
    let mut response = (
        StatusCode::SERVICE_UNAVAILABLE,
        "E_NO_BROKER no publish-ready broker",
    )
        .into_response();
    response.headers_mut().insert(
        header::RETRY_AFTER,
        axum::http::HeaderValue::from_static("1"),
    );
    response
}

fn ambiguous() -> Response {
    (
        StatusCode::BAD_GATEWAY,
        "E_AMBIGUOUS backend may have committed the request; automatic retry is unsafe",
    )
        .into_response()
}

fn body_too_large() -> Response {
    (
        StatusCode::PAYLOAD_TOO_LARGE,
        "E_BAD_BODY body exceeds proxy limit",
    )
        .into_response()
}

fn body_timeout() -> Response {
    (
        StatusCode::REQUEST_TIMEOUT,
        "E_BODY_TIMEOUT proxy request body read timed out",
    )
        .into_response()
}

fn throttled() -> Response {
    let mut response = (
        StatusCode::TOO_MANY_REQUESTS,
        "E_THROTTLED proxy publish byte budget is exhausted",
    )
        .into_response();
    response.headers_mut().insert(
        header::RETRY_AFTER,
        axum::http::HeaderValue::from_static("1"),
    );
    response
}

fn stats_throttled() -> Response {
    let mut response = (
        StatusCode::TOO_MANY_REQUESTS,
        "E_THROTTLED proxy stats byte budget is exhausted",
    )
        .into_response();
    response.headers_mut().insert(
        header::RETRY_AFTER,
        axum::http::HeaderValue::from_static("1"),
    );
    response
}

fn stats_timeout() -> Response {
    (
        StatusCode::GATEWAY_TIMEOUT,
        "E_STATS_TIMEOUT proxy stats collection timed out",
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Backend;
    use tower::ServiceExt;

    #[test]
    fn kodo_stats_ports_override_the_pod_ordinal() {
        let state = ProxyState {
            pool: BackendPool::default(),
            broker_pool: BackendPool::default(),
            client: reqwest::Client::new(),
            max_body_bytes: 1024,
            body_timeout: std::time::Duration::from_secs(1),
            inflight_bytes: Arc::new(Semaphore::new(1024)),
            metrics: ProxyMetrics::default(),
            kodo: Some(KodoConfig {
                ordinal: 2,
                cleanup_enabled: false,
                cleanup_token: None,
                registry_token: None,
            }),
        };
        assert_eq!(state_for_stats_shard(&state, 0).kodo.unwrap().ordinal, 0);
        assert_eq!(state_for_stats_shard(&state, 1).kodo.unwrap().ordinal, 1);
    }

    #[test]
    fn mutating_http_requests_never_fail_over_after_an_ambiguous_send() {
        assert!(!retry_safe_method(&Method::POST));
        assert!(!retry_safe_method(&Method::PUT));
        assert!(retry_safe_method(&Method::GET));
        assert!(retry_safe_method(&Method::HEAD));
    }

    #[tokio::test]
    async fn ping_does_not_depend_on_backend_health() {
        assert_eq!(ping().await, "OK");
    }

    #[tokio::test]
    async fn metrics_expose_backend_inventory() {
        let state = ProxyState {
            pool: BackendPool::default(),
            broker_pool: BackendPool::default(),
            client: reqwest::Client::new(),
            max_body_bytes: 1024,
            body_timeout: std::time::Duration::from_secs(1),
            inflight_bytes: Arc::new(Semaphore::new(1024)),
            metrics: ProxyMetrics::default(),
            kodo: None,
        };
        let body = prometheus(State(state)).await;
        assert!(body.contains("rustqueue_proxy_publish_backends 0\n"));
        assert!(body.contains("rustqueue_proxy_broker_backends 0\n"));
        assert!(body.contains("rustqueue_proxy_kodo_gateway 0\n"));
    }

    #[tokio::test]
    async fn no_backend_is_retryable() {
        let state = ProxyState {
            pool: BackendPool::default(),
            broker_pool: BackendPool::default(),
            client: reqwest::Client::new(),
            max_body_bytes: 1024,
            body_timeout: std::time::Duration::from_secs(1),
            inflight_bytes: Arc::new(Semaphore::new(1024)),
            metrics: ProxyMetrics::default(),
            kodo: None,
        };
        let response = Router::new()
            .fallback(any(forward))
            .with_state(state)
            .oneshot(
                Request::builder()
                    .uri("/pub?topic=x")
                    .body(Body::from(axum::body::Bytes::from_static(b"x")))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers().get(header::RETRY_AFTER).unwrap(), "1");
    }

    #[tokio::test]
    async fn kodo_gateway_does_not_expose_the_http_publish_proxy() {
        let state = ProxyState {
            pool: BackendPool::default(),
            broker_pool: BackendPool::default(),
            client: reqwest::Client::new(),
            max_body_bytes: 1024,
            body_timeout: std::time::Duration::from_secs(1),
            inflight_bytes: Arc::new(Semaphore::new(1024)),
            metrics: ProxyMetrics::default(),
            kodo: Some(KodoConfig {
                ordinal: 0,
                cleanup_enabled: false,
                cleanup_token: None,
                registry_token: None,
            }),
        };
        let response = Router::new()
            .fallback(any(forward))
            .with_state(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/pub?topic=x")
                    .body(Body::from("message"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn kodo_stats_respects_the_node_byte_budget() {
        let state = ProxyState {
            pool: BackendPool::default(),
            broker_pool: BackendPool::default(),
            client: reqwest::Client::new(),
            max_body_bytes: 1024,
            body_timeout: std::time::Duration::from_secs(1),
            inflight_bytes: Arc::new(Semaphore::new(1)),
            metrics: ProxyMetrics::default(),
            kodo: Some(KodoConfig {
                ordinal: 0,
                cleanup_enabled: false,
                cleanup_token: None,
                registry_token: None,
            }),
        };
        let response = Router::new()
            .route("/stats", get(kodo_stats))
            .with_state(state)
            .oneshot(
                Request::builder()
                    .uri("/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers().get(header::RETRY_AFTER).unwrap(), "1");
    }

    #[tokio::test]
    async fn rejects_a_request_that_exceeds_the_node_inflight_budget() {
        let state = ProxyState {
            pool: BackendPool::default(),
            broker_pool: BackendPool::default(),
            client: reqwest::Client::new(),
            max_body_bytes: 1024,
            body_timeout: std::time::Duration::from_secs(1),
            inflight_bytes: Arc::new(Semaphore::new(1)),
            metrics: ProxyMetrics::default(),
            kodo: None,
        };
        let response = Router::new()
            .fallback(any(forward))
            .with_state(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/pub?topic=x")
                    .header(header::CONTENT_LENGTH, "2")
                    .body(Body::from("xx"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers().get(header::RETRY_AFTER).unwrap(), "1");
    }

    #[tokio::test]
    async fn non_kodo_channel_delete_is_forwarded_to_the_backend() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let backend = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/channel/delete",
                    axum::routing::post(|| async { StatusCode::ACCEPTED }),
                ),
            )
            .await
            .unwrap();
        });
        let pool = BackendPool::default();
        pool.replace(vec![Backend {
            broadcast_address: address.ip().to_string(),
            tcp_port: 4150,
            http_port: address.port(),
            node_id: 1,
        }]);
        let state = ProxyState {
            pool,
            broker_pool: BackendPool::default(),
            client: reqwest::Client::new(),
            max_body_bytes: 1024,
            body_timeout: std::time::Duration::from_secs(1),
            inflight_bytes: Arc::new(Semaphore::new(1024)),
            metrics: ProxyMetrics::default(),
            kodo: None,
        };
        let response = Router::new()
            .route("/channel/delete", axum::routing::post(kodo_delete_channel))
            .fallback(any(forward))
            .with_state(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/channel/delete?topic=events&channel=workers")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        backend.abort();
    }
}
