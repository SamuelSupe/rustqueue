use crate::config::Config;
use parking_lot::Mutex;
use regex::Regex;
use reqwest::Client;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("authorization service request failed")]
    Service,
    #[error("authorization service returned an invalid response")]
    InvalidResponse,
    #[error("no permissions found")]
    Unauthorized,
}

pub struct Authenticator {
    client: Client,
    endpoints: Vec<String>,
    max_response_bytes: usize,
    max_ttl_seconds: u64,
    cache: Mutex<AuthCache>,
}

#[derive(Clone)]
pub struct AuthSession {
    pub identity: String,
    pub identity_url: String,
    expires_at: Instant,
    grants: Vec<Grant>,
}

#[derive(Clone)]
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
        for endpoint in &self.endpoints {
            let result = self
                .client
                .get(endpoint)
                .query(&[
                    ("remote_ip", remote_ip),
                    ("tls", if tls { "true" } else { "false" }),
                    ("common_name", common_name),
                    ("auth_secret", secret),
                ])
                .send()
                .await;
            let Ok(mut response) = result else {
                continue;
            };
            if !response.status().is_success() {
                continue;
            }
            if response
                .content_length()
                .is_some_and(|length| length > self.max_response_bytes as u64)
            {
                continue;
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
            let session = AuthSession::try_from_response(response, self.max_ttl_seconds)?;
            self.cache.lock().insert(cache_key, session.clone());
            return Ok(session);
        }
        Err(AuthError::Service)
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
            if let Some(oldest) = self.order.pop_front() {
                self.values.remove(&oldest);
            }
        }
    }
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
    fn try_from_response(response: AuthResponse, max_ttl_seconds: u64) -> Result<Self, AuthError> {
        let mut grants = Vec::with_capacity(response.authorizations.len());
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
            let topic = Regex::new(&authorization.topic).map_err(|_| AuthError::InvalidResponse)?;
            let channels = authorization
                .channels
                .iter()
                .map(|channel| Regex::new(channel).map_err(|_| AuthError::InvalidResponse))
                .collect::<Result<_, _>>()?;
            grants.push(Grant {
                publish,
                subscribe,
                topic,
                channels,
            });
        }
        if grants.is_empty() {
            return Err(AuthError::Unauthorized);
        }
        let ttl_seconds = response.ttl.min(max_ttl_seconds);
        Ok(Self {
            identity: response.identity,
            identity_url: response.identity_url,
            expires_at: Instant::now() + Duration::from_secs(ttl_seconds),
            grants,
        })
    }

    pub fn can_publish(&self, topic: &str) -> bool {
        if Instant::now() >= self.expires_at {
            return false;
        }
        self.grants
            .iter()
            .any(|grant| grant.publish && grant.topic.is_match(topic))
    }

    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }

    pub fn can_subscribe(&self, topic: &str, channel: &str) -> bool {
        if Instant::now() >= self.expires_at {
            return false;
        }
        self.grants.iter().any(|grant| {
            grant.subscribe
                && grant.topic.is_match(topic)
                && grant
                    .channels
                    .iter()
                    .any(|pattern| pattern.is_match(channel))
        })
    }

    pub fn permission_count(&self) -> usize {
        self.grants.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(session.expires_at > Instant::now());
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
        expired.expires_at = Instant::now();
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
                identity: "worker".into(),
                identity_url: String::new(),
                expires_at: Instant::now() + Duration::from_millis(1),
                grants: Vec::new(),
            };
            cache.insert(key, session);
            if let Some(value) = cache.values.get_mut(&key) {
                value.expires_at = Instant::now();
            }
            assert!(cache.get(&key).is_none());
        }
        assert!(cache.order.is_empty());
        assert!(cache.values.is_empty());
    }
}
