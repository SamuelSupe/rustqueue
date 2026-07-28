use crate::config::Config;
use parking_lot::Mutex;
use regex_automata::{meta::Regex, nfa::thompson::WhichCaptures};
use reqwest::Client;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use std::mem::size_of;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const AUTH_MEMORY_UNIT_BYTES: usize = 4 * 1024;
const AUTH_RESPONSE_WORKING_SET_MULTIPLIER: usize = 3;
const AUTH_REGEX_NFA_LIMIT_BYTES: usize = 256 * 1024;
const AUTH_REGEX_ENGINE_LIMIT_BYTES: usize = 64 * 1024;
const AUTH_REGEX_SEARCH_HEADROOM_BYTES: usize = 64 * 1024;
const MAX_AUTHORIZATIONS: usize = 256;
const MAX_AUTH_REGEX_PATTERNS: usize = 256;
const MAX_AUTH_PATTERN_BYTES: usize = 4 * 1024;
const MAX_AUTH_PATTERN_SOURCE_BYTES: usize = 64 * 1024;

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("authorization service request failed")]
    Service,
    #[error("authorization service returned an invalid response")]
    InvalidResponse,
    #[error("no permissions found")]
    Unauthorized,
    #[error("authorization memory budget is exhausted")]
    Overloaded,
}

pub struct Authenticator {
    client: Client,
    endpoints: Vec<String>,
    max_response_bytes: usize,
    max_ttl_seconds: u64,
    cache: Mutex<AuthCache>,
    memory: Arc<Semaphore>,
}

#[derive(Clone)]
pub struct AuthSession {
    inner: Arc<AuthSessionInner>,
}

struct AuthSessionInner {
    identity: String,
    identity_url: String,
    expires_at: Instant,
    grants: Vec<Grant>,
    _memory: Vec<AuthMemoryReservation>,
}

struct Grant {
    publish: bool,
    subscribe: bool,
    topic: Regex,
    channels: Vec<Regex>,
}

struct AuthCache {
    values: HashMap<[u8; 32], AuthSession>,
    order: VecDeque<[u8; 32]>,
    max_entries: usize,
}

struct AuthMemoryReservation {
    _permit: OwnedSemaphorePermit,
}

#[derive(Deserialize)]
struct AuthResponse {
    #[serde(default)]
    ttl: u64,
    #[serde(default)]
    identity: String,
    #[serde(default)]
    identity_url: String,
    #[serde(default)]
    authorizations: Vec<Authorization>,
}

#[derive(Deserialize)]
struct Authorization {
    #[serde(default)]
    permissions: Vec<String>,
    topic: String,
    #[serde(default)]
    channels: Vec<String>,
}

impl Authenticator {
    pub fn new(config: &Config) -> Result<Option<Self>, AuthError> {
        if config.security.auth_http_addresses.is_empty() {
            return Ok(None);
        }
        let client = Client::builder()
            .connect_timeout(Duration::from_millis(
                config.limits.auth_timeout_ms.min(2_000),
            ))
            .timeout(Duration::from_millis(config.limits.auth_timeout_ms))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| AuthError::Service)?;
        let endpoints = config
            .security
            .auth_http_addresses
            .iter()
            .map(|address| {
                let address = if address.starts_with("http://") || address.starts_with("https://") {
                    address.clone()
                } else {
                    format!("http://{address}")
                };
                format!("{}/auth", address.trim_end_matches('/'))
            })
            .collect();
        let memory_units =
            auth_memory_units(config.limits.auth_memory_bytes).ok_or(AuthError::InvalidResponse)?;
        Ok(Some(Self {
            client,
            endpoints,
            max_response_bytes: config.limits.auth_response_bytes,
            max_ttl_seconds: config.limits.auth_max_ttl_seconds,
            cache: Mutex::new(AuthCache {
                values: HashMap::new(),
                order: VecDeque::new(),
                max_entries: config.limits.auth_cache_max_entries,
            }),
            memory: Arc::new(Semaphore::new(memory_units as usize)),
        }))
    }

    pub async fn authenticate(
        &self,
        remote_ip: &str,
        tls: bool,
        common_name: &str,
        secret: &[u8],
    ) -> Result<AuthSession, AuthError> {
        let secret = std::str::from_utf8(secret).map_err(|_| AuthError::Unauthorized)?;
        let cache_key = auth_cache_key(remote_ip, tls, common_name, secret.as_bytes());
        if let Some(session) = self.cache.lock().get(&cache_key) {
            return Ok(session);
        }
        let response_working_set = self
            .max_response_bytes
            .saturating_mul(AUTH_RESPONSE_WORKING_SET_MULTIPLIER);
        let response_memory = self.reserve_memory(response_working_set)?;
        let mut last_error = AuthError::Service;
        for endpoint in &self.endpoints {
            match self
                .authenticate_endpoint(endpoint, remote_ip, tls, common_name, secret)
                .await
            {
                Ok(session) => {
                    drop(response_memory);
                    self.cache.lock().insert(cache_key, session.clone());
                    return Ok(session);
                }
                Err(AuthError::Unauthorized) => return Err(AuthError::Unauthorized),
                Err(AuthError::Overloaded) => return Err(AuthError::Overloaded),
                Err(error) => last_error = error,
            }
        }
        Err(last_error)
    }

    async fn authenticate_endpoint(
        &self,
        endpoint: &str,
        remote_ip: &str,
        tls: bool,
        common_name: &str,
        secret: &str,
    ) -> Result<AuthSession, AuthError> {
        let mut response = self
            .client
            .get(endpoint)
            .query(&[
                ("remote_ip", remote_ip),
                ("tls", if tls { "true" } else { "false" }),
                ("common_name", common_name),
                ("auth_secret", secret),
            ])
            .send()
            .await
            .map_err(|_| AuthError::Service)?;
        if matches!(
            response.status(),
            reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN
        ) {
            // A definitive policy denial must not fall through to another
            // replica that may have a stale or more permissive policy.
            return Err(AuthError::Unauthorized);
        }
        if !response.status().is_success() {
            return Err(AuthError::Service);
        }
        if response
            .content_length()
            .is_some_and(|length| length > self.max_response_bytes as u64)
        {
            return Err(AuthError::InvalidResponse);
        }
        let mut body = Vec::new();
        loop {
            let chunk = response.chunk().await.map_err(|_| AuthError::Service)?;
            let Some(chunk) = chunk else {
                break;
            };
            if body.len().saturating_add(chunk.len()) > self.max_response_bytes {
                return Err(AuthError::InvalidResponse);
            }
            body.extend_from_slice(&chunk);
        }
        let response: AuthResponse =
            serde_json::from_slice(&body).map_err(|_| AuthError::InvalidResponse)?;
        self.build_session(response)
    }

    fn build_session(&self, response: AuthResponse) -> Result<AuthSession, AuthError> {
        AuthSession::try_from_response_with_memory(response, self.max_ttl_seconds, |bytes| {
            self.reserve_memory(bytes)
        })
    }

    fn reserve_memory(&self, bytes: usize) -> Result<AuthMemoryReservation, AuthError> {
        let units = auth_memory_units(bytes).ok_or(AuthError::Overloaded)?;
        loop {
            match Arc::clone(&self.memory).try_acquire_many_owned(units) {
                Ok(permit) => return Ok(AuthMemoryReservation { _permit: permit }),
                Err(_) if self.cache.lock().evict_oldest() => {}
                Err(_) => return Err(AuthError::Overloaded),
            }
        }
    }
}

impl AuthCache {
    fn get(&mut self, key: &[u8; 32]) -> Option<AuthSession> {
        let session = self.values.get(key)?.clone();
        if session.is_expired() {
            self.values.remove(key);
            self.order.retain(|candidate| candidate != key);
            return None;
        }
        Some(session)
    }

    fn insert(&mut self, key: [u8; 32], session: AuthSession) {
        if session.is_expired() {
            return;
        }
        self.values.insert(key, session);
        self.order.retain(|candidate| candidate != &key);
        self.order.push_back(key);
        while self.values.len() > self.max_entries {
            if !self.evict_oldest() {
                break;
            }
        }
    }

    fn evict_oldest(&mut self) -> bool {
        while let Some(oldest) = self.order.pop_front() {
            if self.values.remove(&oldest).is_some() {
                return true;
            }
        }
        false
    }
}

fn auth_memory_units(bytes: usize) -> Option<u32> {
    u32::try_from(bytes.max(1).div_ceil(AUTH_MEMORY_UNIT_BYTES)).ok()
}

fn auth_cache_key(remote_ip: &str, tls: bool, common_name: &str, secret: &[u8]) -> [u8; 32] {
    let mut digest = Sha256::new();
    for part in [
        remote_ip.as_bytes(),
        &[u8::from(tls)],
        common_name.as_bytes(),
        secret,
    ] {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part);
    }
    digest.finalize().into()
}

impl AuthSession {
    #[cfg(test)]
    fn try_from_response(response: AuthResponse, max_ttl_seconds: u64) -> Result<Self, AuthError> {
        Self::build(response, max_ttl_seconds, |_| Ok(None))
    }

    fn try_from_response_with_memory(
        response: AuthResponse,
        max_ttl_seconds: u64,
        mut reserve: impl FnMut(usize) -> Result<AuthMemoryReservation, AuthError>,
    ) -> Result<Self, AuthError> {
        Self::build(response, max_ttl_seconds, |bytes| reserve(bytes).map(Some))
    }

    fn build(
        response: AuthResponse,
        max_ttl_seconds: u64,
        mut reserve: impl FnMut(usize) -> Result<Option<AuthMemoryReservation>, AuthError>,
    ) -> Result<Self, AuthError> {
        let shape = validate_auth_response(&response)?;
        let base_bytes = size_of::<AuthSessionInner>()
            .saturating_add(2 * size_of::<usize>())
            .saturating_add(response.identity.capacity())
            .saturating_add(response.identity_url.capacity())
            .saturating_add(shape.grants.saturating_mul(size_of::<Grant>()))
            .saturating_add(shape.channel_patterns.saturating_mul(size_of::<Regex>()))
            .saturating_add(
                shape
                    .patterns
                    .saturating_add(1)
                    .saturating_mul(size_of::<AuthMemoryReservation>()),
            );
        let mut memory = Vec::with_capacity(shape.patterns.saturating_add(1));
        if let Some(reservation) = reserve(base_bytes)? {
            memory.push(reservation);
        }

        let mut grants = Vec::with_capacity(shape.grants);
        for authorization in response.authorizations {
            let publish = authorization
                .permissions
                .iter()
                .any(|permission| permission == "publish");
            let subscribe = authorization
                .permissions
                .iter()
                .any(|permission| permission == "subscribe");
            if !publish && !subscribe {
                continue;
            }
            let topic = compile_auth_pattern(&authorization.topic)?;
            if let Some(reservation) = reserve(auth_regex_memory_bytes(&topic))? {
                memory.push(reservation);
            }
            let mut channels = Vec::with_capacity(authorization.channels.len());
            for channel in authorization.channels {
                let pattern = compile_auth_pattern(&channel)?;
                if let Some(reservation) = reserve(auth_regex_memory_bytes(&pattern))? {
                    memory.push(reservation);
                }
                channels.push(pattern);
            }
            grants.push(Grant {
                publish,
                subscribe,
                topic,
                channels,
            });
        }
        let ttl_seconds = response.ttl.min(max_ttl_seconds);
        Ok(Self {
            inner: Arc::new(AuthSessionInner {
                identity: response.identity,
                identity_url: response.identity_url,
                expires_at: Instant::now() + Duration::from_secs(ttl_seconds),
                grants,
                _memory: memory,
            }),
        })
    }

    pub fn can_publish(&self, topic: &str) -> bool {
        if self.is_expired() {
            return false;
        }
        self.inner
            .grants
            .iter()
            .any(|grant| grant.publish && grant.topic.is_match(topic))
    }

    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.inner.expires_at
    }

    pub fn can_subscribe(&self, topic: &str, channel: &str) -> bool {
        if self.is_expired() {
            return false;
        }
        self.inner.grants.iter().any(|grant| {
            grant.subscribe
                && grant.topic.is_match(topic)
                && grant
                    .channels
                    .iter()
                    .any(|pattern| pattern.is_match(channel))
        })
    }

    pub fn permission_count(&self) -> usize {
        self.inner.grants.len()
    }

    pub fn identity(&self) -> &str {
        &self.inner.identity
    }

    pub fn identity_url(&self) -> &str {
        &self.inner.identity_url
    }
}

struct AuthResponseShape {
    grants: usize,
    patterns: usize,
    channel_patterns: usize,
}

fn validate_auth_response(response: &AuthResponse) -> Result<AuthResponseShape, AuthError> {
    if response.ttl == 0 || response.authorizations.len() > MAX_AUTHORIZATIONS {
        return Err(AuthError::InvalidResponse);
    }
    let mut grants = 0usize;
    let mut patterns = 0usize;
    let mut channel_patterns = 0usize;
    let mut pattern_bytes = 0usize;
    for authorization in &response.authorizations {
        let relevant = authorization
            .permissions
            .iter()
            .any(|permission| permission == "publish" || permission == "subscribe");
        if !relevant {
            continue;
        }
        grants = grants.checked_add(1).ok_or(AuthError::InvalidResponse)?;
        patterns = patterns
            .checked_add(authorization.channels.len().saturating_add(1))
            .ok_or(AuthError::InvalidResponse)?;
        channel_patterns = channel_patterns
            .checked_add(authorization.channels.len())
            .ok_or(AuthError::InvalidResponse)?;
        if patterns > MAX_AUTH_REGEX_PATTERNS {
            return Err(AuthError::InvalidResponse);
        }
        for pattern in std::iter::once(&authorization.topic).chain(&authorization.channels) {
            if pattern.len() > MAX_AUTH_PATTERN_BYTES {
                return Err(AuthError::InvalidResponse);
            }
            pattern_bytes = pattern_bytes
                .checked_add(pattern.len())
                .ok_or(AuthError::InvalidResponse)?;
            if pattern_bytes > MAX_AUTH_PATTERN_SOURCE_BYTES {
                return Err(AuthError::InvalidResponse);
            }
        }
    }
    if grants == 0 {
        return Err(AuthError::Unauthorized);
    }
    Ok(AuthResponseShape {
        grants,
        patterns,
        channel_patterns,
    })
}

fn compile_auth_pattern(pattern: &str) -> Result<Regex, AuthError> {
    Regex::builder()
        .configure(
            Regex::config()
                .which_captures(WhichCaptures::None)
                .nfa_size_limit(Some(AUTH_REGEX_NFA_LIMIT_BYTES))
                .onepass_size_limit(Some(AUTH_REGEX_ENGINE_LIMIT_BYTES))
                .hybrid_cache_capacity(AUTH_REGEX_ENGINE_LIMIT_BYTES)
                .dfa_size_limit(Some(AUTH_REGEX_ENGINE_LIMIT_BYTES)),
        )
        .build(pattern)
        .map_err(|_| AuthError::InvalidResponse)
}

fn auth_regex_memory_bytes(pattern: &Regex) -> usize {
    size_of::<Regex>()
        .saturating_add(pattern.memory_usage())
        .saturating_add(AUTH_REGEX_SEARCH_HEADROOM_BYTES)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use axum::routing::get;
    use axum::Router;

    #[test]
    fn applies_topic_and_channel_permissions() {
        let session = AuthSession::try_from_response(
            AuthResponse {
                ttl: 10_000,
                identity: "worker".into(),
                identity_url: String::new(),
                authorizations: vec![Authorization {
                    permissions: vec!["subscribe".into(), "publish".into()],
                    topic: "^orders$".into(),
                    channels: vec!["^workers-[0-9]+$".into()],
                }],
            },
            3600,
        )
        .unwrap();
        assert!(session.can_publish("orders"));
        assert!(session.can_subscribe("orders", "workers-2"));
        assert!(!session.can_subscribe("orders", "admin"));
        assert!(session.inner.expires_at > Instant::now());
        assert!(Arc::ptr_eq(&session.inner, &session.clone().inner));
    }

    #[test]
    fn shared_auth_cache_is_bounded_and_skips_expired_sessions() {
        let session = || {
            AuthSession::try_from_response(
                AuthResponse {
                    ttl: 60,
                    identity: "worker".into(),
                    identity_url: String::new(),
                    authorizations: vec![Authorization {
                        permissions: vec!["publish".into()],
                        topic: ".*".into(),
                        channels: Vec::new(),
                    }],
                },
                60,
            )
            .unwrap()
        };
        let mut cache = AuthCache {
            values: HashMap::new(),
            order: VecDeque::new(),
            max_entries: 1,
        };
        cache.insert([1; 32], session());
        cache.insert([2; 32], session());
        assert!(cache.get(&[1; 32]).is_none());
        assert!(cache.get(&[2; 32]).is_some());

        let mut expired = session();
        Arc::get_mut(&mut expired.inner).unwrap().expires_at = Instant::now();
        cache.insert([3; 32], expired);
        assert!(cache.get(&[3; 32]).is_none());
        assert_eq!(cache.order.len(), cache.values.len());
    }

    #[test]
    fn expired_auth_cache_churn_does_not_leak_fifo_entries() {
        let mut cache = AuthCache {
            values: HashMap::new(),
            order: VecDeque::new(),
            max_entries: 2,
        };
        for ordinal in 0..1_000u16 {
            let key = [(ordinal % 251) as u8; 32];
            let session = AuthSession {
                inner: Arc::new(AuthSessionInner {
                    identity: "worker".into(),
                    identity_url: String::new(),
                    expires_at: Instant::now() + Duration::from_millis(1),
                    grants: Vec::new(),
                    _memory: Vec::new(),
                }),
            };
            cache.insert(key, session);
            if let Some(value) = cache.values.get_mut(&key) {
                Arc::get_mut(&mut value.inner).unwrap().expires_at = Instant::now();
            }
            assert!(cache.get(&key).is_none());
        }
        assert!(cache.order.is_empty());
        assert!(cache.values.is_empty());
    }

    #[test]
    fn rejects_auth_responses_that_amplify_regex_compilation() {
        let channels = (0..MAX_AUTH_REGEX_PATTERNS)
            .map(|ordinal| format!("^worker-{ordinal}$"))
            .collect();
        let response = AuthResponse {
            ttl: 60,
            identity: "worker".into(),
            identity_url: String::new(),
            authorizations: vec![Authorization {
                permissions: vec!["subscribe".into()],
                topic: "^orders$".into(),
                channels,
            }],
        };
        assert!(matches!(
            AuthSession::try_from_response(response, 60),
            Err(AuthError::InvalidResponse)
        ));

        let response = AuthResponse {
            ttl: 0,
            identity: "worker".into(),
            identity_url: String::new(),
            authorizations: vec![Authorization {
                permissions: vec!["publish".into()],
                topic: ".*".into(),
                channels: Vec::new(),
            }],
        };
        assert!(matches!(
            AuthSession::try_from_response(response, 60),
            Err(AuthError::InvalidResponse)
        ));
    }

    #[test]
    fn auth_memory_budget_bounds_cached_and_live_sessions() {
        let budget = 2 * 1024 * 1024;
        let mut config = Config::default();
        config.security.auth_http_addresses = vec!["http://127.0.0.1:1".into()];
        config.limits.auth_memory_bytes = budget;
        let authenticator = Authenticator::new(&config).unwrap().unwrap();

        for ordinal in 0..100u8 {
            let session = authenticator
                .build_session(AuthResponse {
                    ttl: 60,
                    identity: format!("worker-{ordinal}"),
                    identity_url: String::new(),
                    authorizations: vec![Authorization {
                        permissions: vec!["publish".into()],
                        topic: format!("^orders-{ordinal}$"),
                        channels: Vec::new(),
                    }],
                })
                .unwrap();
            authenticator.cache.lock().insert([ordinal; 32], session);
        }
        assert!(authenticator.cache.lock().values.len() < 100);

        let held = authenticator.reserve_memory(budget).unwrap();
        assert!(matches!(
            authenticator.reserve_memory(1),
            Err(AuthError::Overloaded)
        ));
        drop(held);
        assert!(authenticator.reserve_memory(1).is_ok());
    }

    #[tokio::test]
    async fn invalid_auth_replica_fails_over_to_the_next_endpoint() {
        let (invalid, invalid_task) = auth_server(StatusCode::OK, "not-json").await;
        let valid_body = r#"{"ttl":60,"identity":"worker","authorizations":[{"permissions":["publish"],"topic":"^orders$","channels":[]}]}"#;
        let (valid, valid_task) = auth_server(StatusCode::OK, valid_body).await;
        let mut config = Config::default();
        config.security.auth_http_addresses = vec![invalid, valid];
        let authenticator = Authenticator::new(&config).unwrap().unwrap();

        let session = authenticator
            .authenticate("127.0.0.1", false, "", b"secret")
            .await
            .unwrap();

        assert!(session.can_publish("orders"));
        invalid_task.abort();
        valid_task.abort();
    }

    #[tokio::test]
    async fn explicit_auth_denial_does_not_fall_through_to_another_replica() {
        let (denied, denied_task) = auth_server(StatusCode::FORBIDDEN, "denied").await;
        let valid_body = r#"{"ttl":60,"identity":"worker","authorizations":[{"permissions":["publish"],"topic":".*","channels":[]}]}"#;
        let (permissive, permissive_task) = auth_server(StatusCode::OK, valid_body).await;
        let mut config = Config::default();
        config.security.auth_http_addresses = vec![denied, permissive];
        let authenticator = Authenticator::new(&config).unwrap().unwrap();

        assert!(matches!(
            authenticator
                .authenticate("127.0.0.1", false, "", b"secret")
                .await,
            Err(AuthError::Unauthorized)
        ));
        denied_task.abort();
        permissive_task.abort();
    }

    async fn auth_server(
        status: StatusCode,
        body: &'static str,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = Router::new().route("/auth", get(move || async move { (status, body) }));
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        (format!("http://{address}"), task)
    }
}
