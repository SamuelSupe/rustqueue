use super::{ensure_enabled, ManagementError};
use crate::app::AppState;
use axum::extract::State;
use axum::http::{header, HeaderMap, HeaderValue};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use serde_json::json;

const COOKIE_NAME: &str = "rustqueue_console_session";

#[derive(Deserialize)]
pub struct UnlockRequest {
    confirmation: String,
}

pub async fn status(State(state): State<AppState>, headers: HeaderMap) -> Json<serde_json::Value> {
    let session = session_id(&headers).and_then(|id| state.sessions.get(&id));
    Json(json!({
        "enabled": state.config.management_enabled,
        "unlocked": session.is_some(),
        "expires_at_ms": session.as_ref().map(|session| session.expires_at_ms),
        "csrf_token": session.as_ref().map(|session| session.csrf.as_str()),
        "confirmation": format!("{}/{}", state.config.namespace, state.config.queue_name),
    }))
}

pub async fn unlock(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<UnlockRequest>,
) -> Result<Response, ManagementError> {
    ensure_enabled(&state)?;
    validate_same_origin_json(&headers)?;
    let expected = format!("{}/{}", state.config.namespace, state.config.queue_name);
    if request.confirmation != expected {
        return Err(ManagementError::bad_request(
            "E_CONFIRMATION_MISMATCH",
            "namespace/queue confirmation does not match",
        ));
    }
    let (id, session) = state.sessions.create();
    let max_age = state.config.management_unlock.as_secs();
    let mut response = Json(json!({
        "enabled": true,
        "unlocked": true,
        "expires_at_ms": session.expires_at_ms,
        "csrf_token": session.csrf,
    }))
    .into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_str(&format!(
            "{COOKIE_NAME}={id}; Path=/api/v1/management; HttpOnly; SameSite=Strict; Max-Age={max_age}"
        ))
        .map_err(|_| ManagementError::internal("build management cookie"))?,
    );
    Ok(response)
}

pub async fn lock(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, ManagementError> {
    ensure_enabled(&state)?;
    let (id, _) = authorize_mutation(&state, &headers)?;
    state.sessions.remove(&id);
    let mut response = Json(json!({"enabled": true, "unlocked": false})).into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_static(
            "rustqueue_console_session=; Path=/api/v1/management; HttpOnly; SameSite=Strict; Max-Age=0",
        ),
    );
    Ok(response)
}

pub fn authorize_mutation(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<(String, String), ManagementError> {
    validate_same_origin_json(headers)?;
    let id = session_id(headers)
        .ok_or_else(|| ManagementError::unauthorized("management session is locked or expired"))?;
    let csrf = headers
        .get("x-rustqueue-csrf")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| ManagementError::unauthorized("CSRF token is required"))?;
    if !state.sessions.validate_csrf(&id, csrf) {
        return Err(ManagementError::unauthorized(
            "management session or CSRF token is invalid",
        ));
    }
    Ok((id, csrf.into()))
}

fn validate_same_origin_json(headers: &HeaderMap) -> Result<(), ManagementError> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if !content_type
        .split(';')
        .next()
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
    {
        return Err(ManagementError::bad_request(
            "E_CONTENT_TYPE",
            "management requests require application/json",
        ));
    }
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| ManagementError::bad_request("E_ORIGIN", "Host header is required"))?;
    let origin = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| ManagementError::bad_request("E_ORIGIN", "Origin header is required"))?;
    let origin_host = origin
        .strip_prefix("http://")
        .or_else(|| origin.strip_prefix("https://"))
        .map(|value| value.trim_end_matches('/'));
    if origin_host != Some(host) {
        return Err(ManagementError::bad_request(
            "E_ORIGIN",
            "cross-origin management requests are rejected",
        ));
    }
    Ok(())
}

fn session_id(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .filter_map(|part| part.trim().split_once('='))
        .find_map(|(name, value)| (name == COOKIE_NAME).then(|| value.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origin_must_match_host_and_json_is_required() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("console:4180"));
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json; charset=utf-8"),
        );
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("http://console:4180"),
        );
        assert!(validate_same_origin_json(&headers).is_ok());
        headers.insert(
            header::ORIGIN,
            HeaderValue::from_static("http://attacker.invalid"),
        );
        assert!(validate_same_origin_json(&headers).is_err());
    }
}
