mod api;
mod app;
mod collector;
mod config;
mod kubernetes;
mod managed_view;
mod management;
mod model;
mod resources;
mod session;
mod state;

use anyhow::Context;
use app::AppState;
use axum::routing::{get, post};
use axum::Router;
use collector::Collector;
use config::Config;
use state::LiveState;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tower_http::services::{ServeDir, ServeFile};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "rustqueue_console=info".into()),
        )
        .json()
        .init();
    let config = Config::from_environment()?;
    let client = kube::Client::try_default()
        .await
        .context("create Kubernetes client")?;
    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_millis(500))
        .timeout(Duration::from_millis(1500))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let live = Arc::new(LiveState::new(config.history_capacity));
    let mutation_lock = Arc::new(Mutex::new(()));
    let state = AppState::new(
        config.clone(),
        client.clone(),
        http.clone(),
        Arc::clone(&live),
        Arc::clone(&mutation_lock),
    );
    tokio::spawn(Collector::new(config.clone(), client, http, live, mutation_lock).run());
    if config.management_enabled {
        tokio::spawn(management::Reconciler::new(state.clone()).run());
    }

    let index = config.static_dir.join("index.html");
    let static_files = ServeDir::new(&config.static_dir).not_found_service(ServeFile::new(index));
    let mut app = Router::new()
        .route("/api/v1/snapshot", get(api::snapshot))
        .route("/api/v1/management", get(management::status))
        .route("/healthz", get(api::health))
        .route("/readyz", get(api::ready))
        .fallback_service(static_files);
    if config.management_enabled {
        app = app
            .route("/api/v1/management/unlock", post(management::unlock))
            .route("/api/v1/management/lock", post(management::lock))
            .route("/api/v1/management/preview", post(management::preview))
            .route("/api/v1/management/apply", post(management::apply));
    }
    let app = app.with_state(state);
    let listener = TcpListener::bind(config.address).await?;
    tracing::info!(address = %config.address, queue = %config.queue_name, namespace = %config.namespace, "RustQueue Console listening");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;
    Ok(())
}
