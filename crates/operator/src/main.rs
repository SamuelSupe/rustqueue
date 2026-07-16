use axum::{response::IntoResponse, routing::get, Router};
use kube::CustomResourceExt;
use rustqueue_operator::RustQueue;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

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
        _ => {
            let leader = Arc::new(AtomicBool::new(false));
            tokio::try_join!(
                rustqueue_operator::controller::run(Arc::clone(&leader)),
                health_server(leader)
            )
            .map(|_| ())
        }
    }
}

async fn health_server(leader: Arc<AtomicBool>) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(|| async { "ok" }))
        .route("/metrics", get(move || metrics(Arc::clone(&leader))));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn metrics(leader: Arc<AtomicBool>) -> impl IntoResponse {
    (
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        format!(
            "# HELP rustqueue_operator_up Whether the operator process is running.\n\
         # TYPE rustqueue_operator_up gauge\n\
         rustqueue_operator_up 1\n\
         # HELP rustqueue_operator_leader Whether this replica holds the leader lease.\n\
         # TYPE rustqueue_operator_leader gauge\n\
         rustqueue_operator_leader {}\n",
            leader.load(Ordering::Acquire) as u8
        ),
    )
}
