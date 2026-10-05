use super::{ApiError, AppState};
use crate::auth::{AuthError, AuthSession, Authenticator};
use axum::extract::ws::{Message, WebSocket};
use axum::http::{header, HeaderMap, StatusCode};
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::time::Duration;

const AUTH_CHANNEL: &str = "websocket#ephemeral";

#[derive(Deserialize)]
struct AuthControl {
    #[serde(rename = "type")]
    kind: String,
    secret: String,
}

pub(super) struct SessionAuth {
    pub(super) session: Option<AuthSession>,
    pub(super) secret: Option<String>,
}

pub(super) struct ControlAuthError {
    pub(super) code: &'static str,
    pub(super) detail: String,
}

pub(super) async fn authenticate_header(
    state: &AppState,
    peer: SocketAddr,
    topic: &str,
    headers: &HeaderMap,
) -> Result<SessionAuth, ApiError> {
    let Some(authenticator) = state.authenticator.as_deref() else {
        return Ok(SessionAuth {
            session: None,
            secret: None,
        });
    };
    let Some(value) = headers.get(header::AUTHORIZATION) else {
        return Ok(SessionAuth {
            session: None,
            secret: None,
        });
    };
    let secret = value
        .to_str()
        .ok()
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|secret| !secret.is_empty())
        .ok_or_else(|| {
            observe_auth_failure(state);
            ApiError {
                status: StatusCode::UNAUTHORIZED,
                code: "E_AUTH_FAILED",
                detail: "Authorization must use a non-empty Bearer token".into(),
            }
        })?
        .to_owned();
    let session = authenticate(authenticator, peer, topic, &secret)
        .await
        .map_err(|error| auth_api_error(state, error))?;
    Ok(SessionAuth {
        session: Some(session),
        secret: Some(secret),
    })
}

pub(super) async fn read_auth(
    socket: &mut WebSocket,
    state: &AppState,
    peer: SocketAddr,
    topic: &str,
) -> Result<SessionAuth, ControlAuthError> {
    let message = tokio::time::timeout(
        Duration::from_millis(state.config.limits.auth_timeout_ms),
        socket.recv(),
    )
    .await
    .map_err(|_| ControlAuthError {
        code: "E_AUTH_TIMEOUT",
        detail: "WebSocket AUTH timed out".into(),
    })?
    .ok_or_else(|| ControlAuthError {
        code: "E_AUTH_FAILED",
        detail: "WebSocket closed before AUTH".into(),
    })?
    .map_err(|error| ControlAuthError {
        code: "E_AUTH_FAILED",
        detail: error.to_string(),
    })?;
    let Message::Text(text) = message else {
        return Err(ControlAuthError {
            code: "E_BAD_AUTH",
            detail: "WebSocket AUTH must be a text control frame".into(),
        });
    };
    let request: AuthControl =
        serde_json::from_str(text.as_str()).map_err(|error| ControlAuthError {
            code: "E_BAD_AUTH",
            detail: error.to_string(),
        })?;
    if request.kind != "auth" || request.secret.is_empty() {
        return Err(ControlAuthError {
            code: "E_BAD_AUTH",
            detail: "WebSocket AUTH frame is invalid".into(),
        });
    }
    let authenticator = state
        .authenticator
        .as_deref()
        .ok_or_else(|| ControlAuthError {
            code: "E_AUTH_DISABLED",
            detail: "WebSocket AUTH is disabled".into(),
        })?;
    let session = authenticate(authenticator, peer, topic, &request.secret)
        .await
        .map_err(|error| ControlAuthError {
            code: auth_error_code(&error),
            detail: "WebSocket AUTH failed".into(),
        })?;
    Ok(SessionAuth {
        session: Some(session),
        secret: Some(request.secret),
    })
}

pub(super) async fn refresh_auth(
    auth: &mut SessionAuth,
    state: &AppState,
    peer: SocketAddr,
    topic: &str,
) -> Result<(), ControlAuthError> {
    let Some(session) = auth.session.as_ref() else {
        return Ok(());
    };
    if !session.is_expired() {
        ensure_authorized(Some(session), topic).map_err(|_| ControlAuthError {
            code: "E_UNAUTHORIZED",
            detail: "WebSocket subscription is no longer authorized".into(),
        })?;
        return Ok(());
    }
    let authenticator = state
        .authenticator
        .as_deref()
        .ok_or_else(|| ControlAuthError {
            code: "E_AUTH_FAILED",
            detail: "WebSocket AUTH configuration is unavailable".into(),
        })?;
    let secret = auth.secret.as_deref().ok_or_else(|| ControlAuthError {
        code: "E_AUTH_FAILED",
        detail: "WebSocket AUTH state is unavailable".into(),
    })?;
    let session = authenticate(authenticator, peer, topic, secret)
        .await
        .map_err(|error| {
            observe_auth_failure(state);
            ControlAuthError {
                code: auth_error_code(&error),
                detail: "WebSocket AUTH refresh failed".into(),
            }
        })?;
    auth.session = Some(session);
    Ok(())
}

async fn authenticate(
    authenticator: &Authenticator,
    peer: SocketAddr,
    topic: &str,
    secret: &str,
) -> Result<AuthSession, AuthError> {
    let session = authenticator
        .authenticate(&peer.ip().to_string(), false, "", secret.as_bytes())
        .await?;
    if !session.can_subscribe(topic, AUTH_CHANNEL) {
        return Err(AuthError::Unauthorized);
    }
    Ok(session)
}

pub(super) fn ensure_authorized(session: Option<&AuthSession>, topic: &str) -> anyhow::Result<()> {
    if session.is_some_and(|session| !session.can_subscribe(topic, AUTH_CHANNEL)) {
        anyhow::bail!("WebSocket subscription is not authorized");
    }
    Ok(())
}

fn auth_api_error(state: &AppState, error: AuthError) -> ApiError {
    observe_auth_failure(state);
    let (status, code, detail) = match error {
        AuthError::Unauthorized => (
            StatusCode::FORBIDDEN,
            "E_UNAUTHORIZED",
            "AUTH does not permit this Topic subscription",
        ),
        AuthError::Overloaded => (
            StatusCode::SERVICE_UNAVAILABLE,
            "E_AUTH_OVERLOADED",
            "AUTH capacity is exhausted",
        ),
        _ => (
            StatusCode::SERVICE_UNAVAILABLE,
            "E_AUTH_FAILED",
            "AUTH service failed",
        ),
    };
    ApiError {
        status,
        code,
        detail: detail.into(),
    }
}

pub(super) fn observe_auth_failure(state: &AppState) {
    state
        .metrics
        .websocket_auth_failures
        .fetch_add(1, Ordering::Relaxed);
    state.metrics.auth_failures.fetch_add(1, Ordering::Relaxed);
}

fn auth_error_code(error: &AuthError) -> &'static str {
    match error {
        AuthError::Unauthorized => "E_UNAUTHORIZED",
        AuthError::Overloaded => "E_AUTH_OVERLOADED",
        _ => "E_AUTH_FAILED",
    }
}
