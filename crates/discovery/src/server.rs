use crate::Directory;
use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::{header, HeaderValue, Request};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use std::net::SocketAddr;

#[derive(Deserialize)]
struct TopicQuery {
    topic: String,
}

#[derive(Deserialize)]
struct ChannelQuery {
    topic: String,
    channel: String,
}

pub fn router(directory: Directory) -> Router {
    Router::new()
        .route("/ping", get(|| async { "OK" }))
        .route("/info", get(info))
        .route("/lookup", get(lookup))
        .route("/topics", get(topics))
        .route("/channels", get(channels))
        .route("/nodes", get(nodes))
        .route("/channel/delete", post(delete_channel))
        .route("/v1/publishers/head", get(publishers_head))
        .route("/v1/publishers", get(publishers))
        .route("/v1/brokers", get(brokers))
        .route("/v1/health", get(health))
        .route("/metrics", get(prometheus))
        .layer(middleware::from_fn(nsq_content_negotiation))
        .with_state(directory)
}

async fn prometheus(State(directory): State<Directory>) -> String {
    let mut output = directory.metrics().render();
    output.push_str(
        "# HELP rustqueue_discovery_source_ready Whether the Kubernetes and Broker registry source is fresh.\n\
# TYPE rustqueue_discovery_source_ready gauge\n\
# HELP rustqueue_discovery_lookup_ready Whether lookup responses have a complete compatible Broker inventory.\n\
# TYPE rustqueue_discovery_lookup_ready gauge\n\
# HELP rustqueue_discovery_brokers Number of consume-ready Brokers in the current inventory.\n\
# TYPE rustqueue_discovery_brokers gauge\n\
# HELP rustqueue_discovery_publishers Number of publish-ready Brokers in the current inventory.\n\
# TYPE rustqueue_discovery_publishers gauge\n",
    );
    output.push_str(&format!(
        "rustqueue_discovery_source_ready {}\n\
rustqueue_discovery_lookup_ready {}\n\
rustqueue_discovery_brokers {}\n\
rustqueue_discovery_publishers {}\n",
        u8::from(directory.source_ready()),
        u8::from(directory.lookup_ready()),
        directory.broker_count(),
        directory.publisher_count(),
    ));
    output
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

async fn lookup(State(directory): State<Directory>, Query(query): Query<TopicQuery>) -> Response {
    // A Broker outage must not block consumers from discovering the remaining
    // topic shards. The complete-inventory signal is still exposed for
    // monitoring and gates operations that require all three Brokers.
    if !directory.source_ready() {
        return source_unavailable();
    }
    Json(json!({
        "channels": directory.channels(&query.topic),
        "producers": directory.producers(Some(&query.topic)),
    }))
    .into_response()
}

async fn topics(State(directory): State<Directory>) -> Response {
    if !directory.source_ready() {
        return source_unavailable();
    }
    Json(json!({"topics": directory.topics()})).into_response()
}

async fn channels(State(directory): State<Directory>, Query(query): Query<TopicQuery>) -> Response {
    if !directory.source_ready() {
        return source_unavailable();
    }
    Json(json!({"channels": directory.channels(&query.topic)})).into_response()
}

async fn nodes(State(directory): State<Directory>) -> Response {
    if !directory.source_ready() || !directory.kodo_nodes_ready() {
        return source_unavailable();
    }
    Json(json!({"producers": directory.node_producers()})).into_response()
}

async fn publishers(State(directory): State<Directory>) -> Response {
    if !directory.source_ready() {
        return source_unavailable();
    }
    let (revision, producers) = directory.publisher_snapshot();
    Json(json!({
        "revision": revision,
        "producers": producers,
    }))
    .into_response()
}

async fn publishers_head(State(directory): State<Directory>) -> Response {
    if !directory.source_ready() {
        return source_unavailable();
    }
    let (revision, broker_count) = directory.publisher_head();
    Json(json!({
        "revision": revision,
        "broker_count": broker_count,
    }))
    .into_response()
}

async fn brokers(State(directory): State<Directory>) -> Response {
    if !directory.source_ready() {
        return source_unavailable();
    }
    let (revision, producers) = directory.broker_snapshot();
    Json(json!({
        "revision": revision,
        "producers": producers,
    }))
    .into_response()
}

fn source_unavailable() -> Response {
    (
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"message": "discovery source is not ready"})),
    )
        .into_response()
}

async fn delete_channel(
    State(directory): State<Directory>,
    Query(query): Query<ChannelQuery>,
) -> Response {
    if !directory.kodo_cleanup_enabled() {
        return (
            axum::http::StatusCode::NOT_FOUND,
            "E_NOT_FOUND Kodo cleanup compatibility is disabled",
        )
            .into_response();
    }
    if query.topic.is_empty() || query.channel.is_empty() {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            "E_BAD_REQUEST topic and channel are required",
        )
            .into_response();
    }
    if directory.broker_count() != 3 {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "E_UNAVAILABLE Kodo cleanup requires all three Brokers",
        )
            .into_response();
    }
    "OK".into_response()
}

async fn health(State(directory): State<Directory>) -> Response {
    let ready = directory.source_ready();
    (
        if ready {
            axum::http::StatusCode::OK
        } else {
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        },
        Json(json!({
            "status": if ready { "ready" } else { "source-unavailable" },
            "broker_count": directory.broker_count(),
            "lookup_ready": directory.lookup_ready()
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Producer;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    #[tokio::test]
    async fn lookup_shape_matches_nsqlookupd() {
        let directory = Directory::default();
        directory.configure_source_health(std::time::Duration::from_secs(5));
        directory.mark_source_success();
        let response = router(directory)
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

    #[tokio::test]
    async fn nodes_use_stable_kodo_gateways_without_changing_lookup() {
        let directory = Directory::default();
        directory.configure_kodo(
            (0..3)
                .map(|ordinal| Producer::gateway("gateway".into(), ordinal))
                .collect(),
            false,
        );
        directory.configure_source_health(std::time::Duration::from_secs(5));
        directory.mark_source_success();
        let response = router(directory)
            .oneshot(
                Request::builder()
                    .uri("/nodes")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        let producers = value["producers"].as_array().unwrap();
        assert_eq!(producers.len(), 3);
        assert!(producers
            .iter()
            .all(|producer| producer["broadcast_address"] == "gateway"));
        assert_eq!(
            producers
                .iter()
                .map(|producer| producer["tcp_port"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            vec![4150, 4152, 4153]
        );
        assert_eq!(
            producers
                .iter()
                .map(|producer| producer["http_port"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            vec![4151, 4154, 4155]
        );
    }

    #[tokio::test]
    async fn kodo_lookup_serves_healthy_shards_while_inventory_is_incomplete() {
        let directory = Directory::default();
        directory.configure_source_health(std::time::Duration::from_secs(5));
        directory.configure_kodo(
            (0..3)
                .map(|ordinal| Producer::gateway("gateway".into(), ordinal))
                .collect(),
            false,
        );
        for node_id in 1..=2 {
            let endpoint = crate::BrokerEndpoint {
                address: format!("127.0.0.{node_id}").parse().unwrap(),
                http_port: 4151,
            };
            directory.observe(
                endpoint,
                crate::BrokerRegistry {
                    format: 7,
                    revision: node_id,
                    node_id,
                    ready: true,
                    publish_ready: true,
                    consume_ready: true,
                    broadcast_address: format!("broker-{node_id}"),
                    tcp_port: 4150,
                    http_port: 4151,
                    stored_messages: 0,
                    depth: 0,
                    in_flight: 0,
                    topics: vec![crate::RegistryTopic {
                        name: "events".into(),
                        paused: false,
                        channels: vec!["workers".into()],
                        stored_messages: 0,
                    }],
                    compatibility: None,
                },
            );
        }
        directory.mark_source_success();

        let lookup = router(directory.clone())
            .oneshot(
                Request::builder()
                    .uri("/lookup?topic=events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(lookup.status(), axum::http::StatusCode::OK);
        let body = axum::body::to_bytes(lookup.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["producers"].as_array().unwrap().len(), 2);
        assert!(!directory.lookup_ready());
        let nodes = router(directory.clone())
            .oneshot(
                Request::builder()
                    .uri("/nodes")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(nodes.status(), axum::http::StatusCode::OK);

        let endpoint = crate::BrokerEndpoint {
            address: "127.0.0.3".parse().unwrap(),
            http_port: 4151,
        };
        directory.observe(
            endpoint,
            crate::BrokerRegistry {
                format: 7,
                revision: 3,
                node_id: 3,
                ready: true,
                publish_ready: true,
                consume_ready: true,
                broadcast_address: "broker-3".into(),
                tcp_port: 4150,
                http_port: 4151,
                stored_messages: 0,
                depth: 0,
                in_flight: 0,
                topics: vec![crate::RegistryTopic {
                    name: "events".into(),
                    paused: false,
                    channels: vec!["workers".into()],
                    stored_messages: 0,
                }],
                compatibility: None,
            },
        );
        let lookup = router(directory.clone())
            .oneshot(
                Request::builder()
                    .uri("/lookup?topic=events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(lookup.status(), axum::http::StatusCode::OK);
        let body = axum::body::to_bytes(lookup.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["producers"].as_array().unwrap().len(), 3);
        assert!(directory.lookup_ready());
    }

    #[tokio::test]
    async fn nodes_and_health_fail_closed_until_the_source_is_ready() {
        let directory = Directory::default();
        directory.configure_source_health(std::time::Duration::from_secs(5));
        directory.configure_kodo(Vec::new(), false);
        for uri in [
            "/nodes",
            "/lookup?topic=events",
            "/topics",
            "/channels?topic=events",
            "/v1/publishers/head",
            "/v1/publishers",
            "/v1/brokers",
            "/v1/health",
        ] {
            let response = router(directory.clone())
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                axum::http::StatusCode::SERVICE_UNAVAILABLE
            );
        }
        directory.mark_source_success();
        let response = router(directory)
            .oneshot(
                Request::builder()
                    .uri("/v1/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
    }

    #[tokio::test]
    async fn cleanup_fails_closed_without_the_complete_broker_inventory() {
        let directory = Directory::default();
        directory.configure_kodo(
            (0..3)
                .map(|ordinal| Producer::gateway(format!("gateway-{ordinal}"), ordinal))
                .collect(),
            true,
        );
        let response = router(directory)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/channel/delete?topic=events&channel=workers")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn metrics_expose_source_and_lookup_readiness() {
        let directory = Directory::default();
        directory.configure_source_health(std::time::Duration::from_secs(5));
        directory.mark_source_success();
        let response = router(directory)
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("rustqueue_discovery_source_ready 1\n"));
        assert!(body.contains("rustqueue_discovery_lookup_ready 1\n"));
    }
}
