use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone)]
pub(in crate::http) struct WebSocketRuntime {
    connections: Arc<Semaphore>,
    frame_bytes: Arc<Semaphore>,
    topics: Arc<Mutex<HashMap<String, usize>>>,
    max_connections_per_topic: usize,
    next_id: Arc<AtomicU64>,
}

pub(super) struct ConnectionAdmission {
    runtime: WebSocketRuntime,
    topic: String,
    _connection: OwnedSemaphorePermit,
}

#[derive(Debug)]
pub(super) enum AdmissionError {
    Global,
    Topic,
}

impl WebSocketRuntime {
    pub(in crate::http) fn new(config: &crate::config::WebSocketConfig) -> Self {
        Self {
            connections: Arc::new(Semaphore::new(config.max_connections)),
            frame_bytes: Arc::new(Semaphore::new(config.frame_inflight_bytes)),
            topics: Arc::new(Mutex::new(HashMap::new())),
            max_connections_per_topic: config.max_connections_per_topic,
            next_id: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(super) fn admit(&self, topic: &str) -> Result<(u64, ConnectionAdmission), AdmissionError> {
        let connection = Arc::clone(&self.connections)
            .try_acquire_owned()
            .map_err(|_| AdmissionError::Global)?;
        let mut topics = self.topics.lock();
        let count = topics.entry(topic.to_owned()).or_default();
        if *count >= self.max_connections_per_topic {
            return Err(AdmissionError::Topic);
        }
        *count += 1;
        let id = self.next_id.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        Ok((
            id,
            ConnectionAdmission {
                runtime: self.clone(),
                topic: topic.to_owned(),
                _connection: connection,
            },
        ))
    }

    pub(super) fn reserve_frame(&self, bytes: usize) -> Option<OwnedSemaphorePermit> {
        let permits = u32::try_from(bytes.max(1)).ok()?;
        Arc::clone(&self.frame_bytes)
            .try_acquire_many_owned(permits)
            .ok()
    }
}

impl Drop for ConnectionAdmission {
    fn drop(&mut self) {
        let mut topics = self.runtime.topics.lock();
        let remove = topics.get_mut(&self.topic).is_some_and(|count| {
            *count = count.saturating_sub(1);
            *count == 0
        });
        if remove {
            topics.remove(&self.topic);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runtime(max_connections: usize, max_connections_per_topic: usize) -> WebSocketRuntime {
        let config = crate::config::WebSocketConfig {
            max_connections,
            max_connections_per_topic,
            ..Default::default()
        };
        WebSocketRuntime::new(&config)
    }

    #[test]
    fn admission_enforces_global_and_per_topic_limits_and_releases() {
        let runtime = runtime(2, 1);
        let (_, first) = runtime.admit("events").unwrap();
        assert!(matches!(
            runtime.admit("events"),
            Err(AdmissionError::Topic)
        ));
        let (_, second) = runtime.admit("other").unwrap();
        assert!(matches!(
            runtime.admit("third"),
            Err(AdmissionError::Global)
        ));

        drop(first);
        let (_, replacement) = runtime.admit("events").unwrap();
        assert!(matches!(
            runtime.admit("other"),
            Err(AdmissionError::Global)
        ));

        drop(second);
        drop(replacement);
        assert!(runtime.admit("events").is_ok());
    }
}
