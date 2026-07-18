use crate::backend::BackendPool;
use crate::metrics::ProxyMetrics;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{header, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::{Json, Router};
use rustqueue_proxy::{parse_forward_metadata, ForwardMetadataError};
use serde_json::json;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Semaphore;

const MAX_BACKEND_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone)]
struct ProxyState {
    pool: BackendPool,
    client: reqwest::Client,
    max_body_bytes: usize,
    inflight_bytes: Arc<Semaphore>,
    metrics: ProxyMetrics,
}

pub async fn serve(
    address: SocketAddr,
    pool: BackendPool,
    max_body_bytes: usize,
    max_inflight_bytes: usize,
    metrics: ProxyMetrics,
) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_millis(500))
        .timeout(std::time::Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let state = ProxyState {
        pool,
        client,
        max_body_bytes,
        inflight_bytes: Arc::new(Semaphore::new(max_inflight_bytes)),
        metrics,
    };
    let router = Router::new()
        .route("/v1/health", get(health))
        .route("/metrics", get(prometheus))
        .fallback(any(forward))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(address).await?;
    tracing::info!(%address, "producer HTTP proxy listening");
    axum::serve(listener, router).await?;
    Ok(())
}

async fn prometheus(State(state): State<ProxyState>) -> String {
    state.metrics.render()
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
    let body = match axum::body::to_bytes(body, state.max_body_bytes).await {
        Ok(body) => body,
        Err(_) => return body_too_large(),
    };
    let path = metadata.path_and_query;
    let backends = state.pool.shuffled(2);
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
            Ok(response) => return backend_response(response).await,
            Err(error) => last_error = Some(error),
        }
    }
    if let Some(error) = last_error {
        tracing::debug!(%error, "all producer HTTP backends failed");
    }
    unavailable()
}

async fn backend_response(response: reqwest::Response) -> Response {
    let status = response.status();
    let headers = response.headers().clone();
    let body = match read_body_bounded(response, MAX_BACKEND_RESPONSE_BYTES).await {
        Some(body) => body,
        None => {
            tracing::debug!("backend response body failed or exceeded its limit");
            return unavailable();
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

fn body_too_large() -> Response {
    (
        StatusCode::PAYLOAD_TOO_LARGE,
        "E_BAD_BODY body exceeds proxy limit",
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

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;

    #[tokio::test]
    async fn no_backend_is_retryable() {
        let state = ProxyState {
            pool: BackendPool::default(),
            client: reqwest::Client::new(),
            max_body_bytes: 1024,
            inflight_bytes: Arc::new(Semaphore::new(1024)),
            metrics: ProxyMetrics::default(),
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
    async fn rejects_a_request_that_exceeds_the_node_inflight_budget() {
        let state = ProxyState {
            pool: BackendPool::default(),
            client: reqwest::Client::new(),
            max_body_bytes: 1024,
            inflight_bytes: Arc::new(Semaphore::new(1)),
            metrics: ProxyMetrics::default(),
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
}
