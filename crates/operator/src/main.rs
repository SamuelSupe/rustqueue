use axum::{response::IntoResponse, routing::get, Router};
use kube::CustomResourceExt;
use rustqueue_operator::RustQueue;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "rustqueue_operator=info".into()),
        )
        .json()
        .init();
    match std::env::args().nth(1).as_deref() {
        Some("crd") => {
            print!("{}", serde_yaml::to_string(&RustQueue::crd())?);
            Ok(())
        }
        Some(command) if command != "run" => anyhow::bail!("unknown command {command}"),
        _ => tokio::try_join!(rustqueue_operator::controller::run(), health_server()).map(|_| ()),
    }
}

async fn health_server() -> anyhow::Result<()> {
    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(|| async { "ok" }))
        .route("/metrics", get(metrics));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn metrics() -> impl IntoResponse {
    (
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        "# HELP rustqueue_operator_up Whether the operator process is running.\n\
         # TYPE rustqueue_operator_up gauge\n\
         rustqueue_operator_up 1\n",
    )
}
