use crate::Directory;
use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{header, HeaderValue, Request};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use std::net::SocketAddr;

#[derive(Deserialize)]
struct TopicQuery {
    topic: String,
}

pub fn router(directory: Directory) -> Router {
    Router::new()
        .route("/ping", get(|| async { "OK" }))
        .route("/info", get(info))
        .route("/lookup", get(lookup))
        .route("/topics", get(topics))
        .route("/channels", get(channels))
        .route("/nodes", get(nodes))
        .route("/v1/publishers", get(publishers))
        .route("/v1/health", get(health))
        .layer(middleware::from_fn(nsq_content_negotiation))
        .with_state(directory)
}

async fn nsq_content_negotiation(request: Request<Body>, next: Next) -> Response {
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

pub async fn serve(address: SocketAddr, directory: Directory) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(address).await?;
    tracing::info!(%address, "RustQueue discovery API listening");
    axum::serve(listener, router(directory)).await?;
    Ok(())
}

async fn info(State(directory): State<Directory>) -> Json<Value> {
    Json(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "broadcast_address": "rustqueue-discovery",
        "hostname": "rustqueue-discovery",
        "tcp_port": 0,
        "http_port": 4161,
        "broker_count": directory.broker_count(),
    }))
}

async fn lookup(
    State(directory): State<Directory>,
    Query(query): Query<TopicQuery>,
) -> Json<Value> {
    Json(json!({
        "channels": directory.channels(&query.topic),
        "producers": directory.producers(Some(&query.topic)),
    }))
}

async fn topics(State(directory): State<Directory>) -> Json<Value> {
    Json(json!({"topics": directory.topics()}))
}

async fn channels(
    State(directory): State<Directory>,
    Query(query): Query<TopicQuery>,
) -> Json<Value> {
    Json(json!({"channels": directory.channels(&query.topic)}))
}

async fn nodes(State(directory): State<Directory>) -> Json<Value> {
    Json(json!({"producers": directory.producers(None)}))
}

async fn publishers(State(directory): State<Directory>) -> Json<Value> {
    Json(json!({"producers": directory.publishers()}))
}

async fn health(State(directory): State<Directory>) -> Json<Value> {
    Json(json!({"status": "ready", "broker_count": directory.broker_count()}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    #[tokio::test]
    async fn lookup_shape_matches_nsqlookupd() {
        let response = router(Directory::default())
            .oneshot(
                Request::builder()
                    .uri("/lookup?topic=events")
                    .header("accept", "application/vnd.nsq; version=1.0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert_eq!(
            response.headers().get("x-nsq-content-type").unwrap(),
            "nsq; version=1.0"
        );
    }
}
