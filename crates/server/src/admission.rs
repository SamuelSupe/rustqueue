use crate::metrics::Metrics;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const PERMIT_BYTES: usize = 4096;
const RECORD_FIXED_BYTES: usize = rustqueue_storage::HEADER_LEN + 4;
const MESSAGE_WORKING_BYTES: usize = 128;

#[derive(Clone, Copy)]
pub enum PublishShape {
    Single,
    Multi,
}

pub struct PublishAdmission {
    permits: Arc<Semaphore>,
    metrics: Arc<Metrics>,
    storage_ready: AtomicBool,
}

pub struct ConnectionBudget {
    permits: Arc<Semaphore>,
}

pub struct PublishReservation {
    bytes: usize,
    metrics: Arc<Metrics>,
    _node: OwnedSemaphorePermit,
    _connection: Option<OwnedSemaphorePermit>,
}

impl std::fmt::Debug for PublishReservation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PublishReservation")
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

impl PublishAdmission {
    pub fn new(capacity_bytes: usize, metrics: Arc<Metrics>) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(units(capacity_bytes))),
            metrics,
            storage_ready: AtomicBool::new(true),
        }
    }

    pub fn set_storage_ready(&self, ready: bool) {
        self.storage_ready.store(ready, Ordering::Release);
    }

    pub fn storage_ready(&self) -> bool {
        self.storage_ready.load(Ordering::Acquire)
    }

    pub fn try_reserve(&self, bytes: usize) -> Option<PublishReservation> {
        self.try_reserve_inner(bytes, None)
    }

    pub fn try_reserve_publish(
        &self,
        bytes: usize,
        shape: PublishShape,
    ) -> Option<PublishReservation> {
        self.try_reserve(working_set_bytes(bytes, shape))
    }

    pub fn try_reserve_connection(
        &self,
        bytes: usize,
        connection: &ConnectionBudget,
    ) -> Option<PublishReservation> {
        let connection = match connection.try_acquire(bytes) {
            Some(permit) => Some(permit),
            None => {
                self.record_rejected(bytes);
                return None;
            }
        };
        self.try_reserve_inner(bytes, connection)
    }

    pub fn try_reserve_connection_publish(
        &self,
        bytes: usize,
        shape: PublishShape,
        connection: &ConnectionBudget,
    ) -> Option<PublishReservation> {
        self.try_reserve_connection(working_set_bytes(bytes, shape), connection)
    }

    fn try_reserve_inner(
        &self,
        bytes: usize,
        connection: Option<OwnedSemaphorePermit>,
    ) -> Option<PublishReservation> {
        if !self.storage_ready() {
            self.record_rejected(bytes);
            return None;
        }
        let count = u32::try_from(units(bytes)).ok()?;
        let node = match Arc::clone(&self.permits).try_acquire_many_owned(count) {
            Ok(permit) => permit,
            Err(_) => {
                self.record_rejected(bytes);
                return None;
            }
        };
        self.metrics
            .publish_inflight_bytes
            .fetch_add(bytes as i64, Ordering::Relaxed);
        Some(PublishReservation {
            bytes,
            metrics: Arc::clone(&self.metrics),
            _node: node,
            _connection: connection,
        })
    }

    fn record_rejected(&self, bytes: usize) {
        self.metrics
            .publish_throttled_requests
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .publish_throttled_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

impl ConnectionBudget {
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(units(capacity_bytes))),
        }
    }

    fn try_acquire(&self, bytes: usize) -> Option<OwnedSemaphorePermit> {
        let count = u32::try_from(units(bytes)).ok()?;
        Arc::clone(&self.permits).try_acquire_many_owned(count).ok()
    }
}

impl Drop for PublishReservation {
    fn drop(&mut self) {
        self.metrics
            .publish_inflight_bytes
            .fetch_sub(self.bytes as i64, Ordering::Relaxed);
    }
}

fn units(bytes: usize) -> usize {
    bytes.max(1).div_ceil(PERMIT_BYTES)
}

pub(crate) fn working_set_bytes(bytes: usize, shape: PublishShape) -> usize {
    let messages = match shape {
        PublishShape::Single => 1,
        PublishShape::Multi => rustqueue_protocol::MAX_MPUB_MESSAGES.min(bytes.max(1)),
    };
    bytes
        .saturating_add(RECORD_FIXED_BYTES)
        .saturating_add(messages.saturating_mul(MESSAGE_WORKING_BYTES))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admission_is_bounded_by_bytes_and_releases_on_drop() {
        let metrics = Arc::new(Metrics::default());
        let admission = PublishAdmission::new(8192, Arc::clone(&metrics));
        let first = admission.try_reserve(5000).unwrap();
        assert!(admission.try_reserve(4096).is_none());
        assert_eq!(metrics.publish_inflight_bytes.load(Ordering::Relaxed), 5000);
        drop(first);
        assert!(admission.try_reserve(8192).is_some());
    }

    #[test]
    fn publish_reservation_charges_internal_metadata_without_body_copies() {
        assert_eq!(
            working_set_bytes(20 * 1024 * 1024, PublishShape::Single),
            20 * 1024 * 1024 + RECORD_FIXED_BYTES + MESSAGE_WORKING_BYTES
        );
        assert!(working_set_bytes(64 * 1024 * 1024, PublishShape::Multi) > 64 * 1024 * 1024);
    }
}
