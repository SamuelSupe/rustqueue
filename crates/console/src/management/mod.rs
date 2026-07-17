mod apply;
mod backend;
mod event;
mod operation_state;
mod preview;
mod reconcile;
mod resource_state;
mod security;

use crate::app::AppState;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

pub use apply::apply;
pub use preview::preview;
pub use reconcile::Reconciler;
pub use security::{lock, status, unlock};

#[derive(Debug)]
pub struct ManagementError {
    status: StatusCode,
    code: &'static str,
    detail: String,
}

impl ManagementError {
    pub fn bad_request(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code,
            detail: detail.into(),
        }
    }

    pub fn unauthorized(detail: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: "E_MANAGEMENT_LOCKED",
            detail: detail.into(),
        }
    }

    pub fn conflict(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code,
            detail: detail.into(),
        }
    }

    pub fn unavailable(detail: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "E_MANAGEMENT_UNAVAILABLE",
            detail: detail.into(),
        }
    }

    pub fn internal(detail: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "E_MANAGEMENT_INTERNAL",
            detail: detail.into(),
        }
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }

    pub fn retryable(&self) -> bool {
        self.status == StatusCode::SERVICE_UNAVAILABLE
            || self.status == StatusCode::INTERNAL_SERVER_ERROR
    }
}

impl IntoResponse for ManagementError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({"code": self.code, "detail": self.detail})),
        )
            .into_response()
    }
}

pub fn ensure_enabled(state: &AppState) -> Result<(), ManagementError> {
    if state.config.management_enabled {
        Ok(())
    } else {
        Err(ManagementError {
            status: StatusCode::NOT_FOUND,
            code: "E_MANAGEMENT_DISABLED",
            detail: "console management is disabled".into(),
        })
    }
}
