use crate::app::AppState;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

pub async fn snapshot(State(state): State<AppState>) -> Response {
    match state.live.snapshot() {
        Some(snapshot) => Json(snapshot).into_response(),
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "collecting",
                "message": "The first cluster snapshot is not ready"
            })),
        )
            .into_response(),
    }
}

pub async fn health() -> Json<serde_json::Value> {
    Json(json!({"status": "ok"}))
}

pub async fn ready(State(state): State<AppState>) -> Response {
    if state.live.snapshot().is_some() {
        Json(json!({"status": "ready"})).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "collecting"})),
        )
            .into_response()
    }
}
