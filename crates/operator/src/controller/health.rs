use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Default)]
pub struct HealthState {
    ready: AtomicBool,
    reconciles: AtomicU64,
    errors: AtomicU64,
    last_success_unix: AtomicI64,
}

impl HealthState {
    pub fn record_success(&self) {
        self.ready.store(true, Ordering::Release);
        self.reconciles.fetch_add(1, Ordering::Relaxed);
        self.last_success_unix
            .store(crate::status::unix_now(), Ordering::Release);
    }

    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }
}

pub async fn spawn(state: Arc<HealthState>) -> anyhow::Result<()> {
    let address = std::env::var("OPERATOR_HTTP_ADDRESS").unwrap_or_else(|_| "0.0.0.0:8080".into());
    let listener = tokio::net::TcpListener::bind(&address).await?;
    let router = Router::new()
        .route("/healthz", get(|| async { "ok\n" }))
        .route("/readyz", get(ready))
        .route("/metrics", get(metrics))
        .with_state(state);
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, router).await {
            tracing::error!(%error, "Operator health server stopped")
        }
    });
    Ok(())
}

async fn ready(State(state): State<Arc<HealthState>>) -> (StatusCode, &'static str) {
    if state.ready.load(Ordering::Acquire) {
        (StatusCode::OK, "ready\n")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready\n")
    }
}

async fn metrics(State(state): State<Arc<HealthState>>) -> String {
    format!(
        "# TYPE rustqueue_operator_reconciles_total counter\n\
         rustqueue_operator_reconciles_total {}\n\
         # TYPE rustqueue_operator_reconcile_errors_total counter\n\
         rustqueue_operator_reconcile_errors_total {}\n\
         # TYPE rustqueue_operator_last_success_unixtime gauge\n\
         rustqueue_operator_last_success_unixtime {}\n",
        state.reconciles.load(Ordering::Relaxed),
        state.errors.load(Ordering::Relaxed),
        state.last_success_unix.load(Ordering::Acquire),
    )
}
