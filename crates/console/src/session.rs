use rand::distributions::{Alphanumeric, DistString};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

const MAX_SESSIONS: usize = 1_024;
const MAX_CHALLENGES_PER_SESSION: usize = 64;

#[derive(Clone, Debug)]
pub struct SessionView {
    pub csrf: String,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug)]
pub struct ActionChallenge {
    pub token: String,
    pub kind: String,
    pub action: String,
    pub topic: String,
    pub channel: Option<String>,
    pub subject_uid: String,
    pub resource_version: String,
    pub subject_kind: String,
    pub owners: Vec<String>,
    pub confirmation: Option<String>,
    pub expires_at_ms: u64,
}

pub struct SessionStore {
    ttl: Duration,
    inner: Mutex<HashMap<String, Session>>,
}

struct Session {
    csrf: String,
    expires_at_ms: u64,
    challenges: HashMap<String, ActionChallenge>,
}

impl SessionStore {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            inner: Mutex::new(HashMap::new()),
        }
    }

    pub fn create(&self) -> (String, SessionView) {
        let id = random_token(48);
        let view = SessionView {
            csrf: random_token(48),
            expires_at_ms: now_ms().saturating_add(self.ttl.as_millis() as u64),
        };
        let mut sessions = self.inner.lock().expect("session lock poisoned");
        purge_expired(&mut sessions);
        if sessions.len() >= MAX_SESSIONS {
            if let Some(oldest) = sessions
                .iter()
                .min_by_key(|(_, session)| session.expires_at_ms)
                .map(|(id, _)| id.clone())
            {
                sessions.remove(&oldest);
            }
        }
        sessions.insert(
            id.clone(),
            Session {
                csrf: view.csrf.clone(),
                expires_at_ms: view.expires_at_ms,
                challenges: HashMap::new(),
            },
        );
        (id, view)
    }

    pub fn get(&self, id: &str) -> Option<SessionView> {
        let mut sessions = self.inner.lock().expect("session lock poisoned");
        purge_expired(&mut sessions);
        sessions.get(id).map(|session| SessionView {
            csrf: session.csrf.clone(),
            expires_at_ms: session.expires_at_ms,
        })
    }

    pub fn remove(&self, id: &str) {
        self.inner.lock().expect("session lock poisoned").remove(id);
    }

    pub fn validate_csrf(&self, id: &str, csrf: &str) -> bool {
        self.get(id)
            .is_some_and(|session| constant_time_eq(session.csrf.as_bytes(), csrf.as_bytes()))
    }

    pub fn issue_challenge(
        &self,
        id: &str,
        mut challenge: ActionChallenge,
    ) -> Option<ActionChallenge> {
        let mut sessions = self.inner.lock().expect("session lock poisoned");
        purge_expired(&mut sessions);
        let session = sessions.get_mut(id)?;
        session
            .challenges
            .retain(|_, item| item.expires_at_ms > now_ms());
        if session.challenges.len() >= MAX_CHALLENGES_PER_SESSION {
            if let Some(oldest) = session
                .challenges
                .iter()
                .min_by_key(|(_, challenge)| challenge.expires_at_ms)
                .map(|(token, _)| token.clone())
            {
                session.challenges.remove(&oldest);
            }
        }
        challenge.token = random_token(48);
        session
            .challenges
            .insert(challenge.token.clone(), challenge.clone());
        Some(challenge)
    }

    pub fn take_challenge(&self, id: &str, token: &str) -> Option<ActionChallenge> {
        let mut sessions = self.inner.lock().expect("session lock poisoned");
        purge_expired(&mut sessions);
        sessions
            .get_mut(id)?
            .challenges
            .remove(token)
            .filter(|challenge| challenge.expires_at_ms > now_ms())
    }
}

fn purge_expired(sessions: &mut HashMap<String, Session>) {
    let now = now_ms();
    sessions.retain(|_, session| session.expires_at_ms > now);
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

fn random_token(length: usize) -> String {
    Alphanumeric.sample_string(&mut rand::thread_rng(), length)
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .fold(0u8, |diff, (left, right)| diff | (left ^ right))
            == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_expiry_is_not_extended_by_reads() {
        let store = SessionStore::new(Duration::from_secs(60));
        let (id, original) = store.create();
        assert_eq!(
            store.get(&id).unwrap().expires_at_ms,
            original.expires_at_ms
        );
        assert_eq!(
            store.get(&id).unwrap().expires_at_ms,
            original.expires_at_ms
        );
    }

    #[test]
    fn challenges_are_single_use() {
        let store = SessionStore::new(Duration::from_secs(60));
        let (id, _) = store.create();
        let challenge = store
            .issue_challenge(
                &id,
                ActionChallenge {
                    token: String::new(),
                    kind: "topic".into(),
                    action: "delete".into(),
                    topic: "orders".into(),
                    channel: None,
                    subject_uid: "uid".into(),
                    resource_version: "1".into(),
                    subject_kind: "topic".into(),
                    owners: vec!["broker-0".into()],
                    confirmation: Some("orders".into()),
                    expires_at_ms: now_ms() + 60_000,
                },
            )
            .unwrap();
        assert!(store.take_challenge(&id, &challenge.token).is_some());
        assert!(store.take_challenge(&id, &challenge.token).is_none());
    }

    #[test]
    fn sessions_and_challenges_are_bounded() {
        let store = SessionStore::new(Duration::from_secs(60));
        for _ in 0..(MAX_SESSIONS + 32) {
            store.create();
        }
        assert_eq!(
            store.inner.lock().expect("session lock poisoned").len(),
            MAX_SESSIONS
        );

        let (id, _) = store.create();
        for ordinal in 0..(MAX_CHALLENGES_PER_SESSION + 8) {
            store
                .issue_challenge(
                    &id,
                    ActionChallenge {
                        token: String::new(),
                        kind: "topic".into(),
                        action: "pause".into(),
                        topic: format!("topic-{ordinal}"),
                        channel: None,
                        subject_uid: "uid".into(),
                        resource_version: "1".into(),
                        subject_kind: "topic".into(),
                        owners: vec!["broker-0".into()],
                        confirmation: None,
                        expires_at_ms: now_ms() + 60_000 + ordinal as u64,
                    },
                )
                .unwrap();
        }
        assert_eq!(
            store
                .inner
                .lock()
                .expect("session lock poisoned")
                .get(&id)
                .unwrap()
                .challenges
                .len(),
            MAX_CHALLENGES_PER_SESSION
        );
    }
}
