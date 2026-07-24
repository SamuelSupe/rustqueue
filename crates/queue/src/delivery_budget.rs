use crate::BrokerError;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const PAYLOAD_READ_WORKING_SET_FACTOR: usize = 2;

pub struct DeliveryHold {
    _permit: Option<OwnedSemaphorePermit>,
    bytes: u64,
    in_flight: Arc<AtomicU64>,
}

pub(crate) struct DeliveryBudget {
    capacity: usize,
    permits: Arc<Semaphore>,
    in_flight: Arc<AtomicU64>,
    waiters: Arc<AtomicUsize>,
    waits_total: AtomicU64,
}

impl DeliveryBudget {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            permits: Arc::new(Semaphore::new(capacity)),
            in_flight: Arc::new(AtomicU64::new(0)),
            waiters: Arc::new(AtomicUsize::new(0)),
            waits_total: AtomicU64::new(0),
        }
    }

    pub(crate) fn max_payload_bytes(&self) -> usize {
        self.capacity / PAYLOAD_READ_WORKING_SET_FACTOR
    }

    pub(crate) async fn acquire(&self, payload_bytes: usize) -> Result<DeliveryHold, BrokerError> {
        if payload_bytes == 0 {
            return Ok(DeliveryHold {
                _permit: None,
                bytes: 0,
                in_flight: Arc::clone(&self.in_flight),
            });
        }
        // A cache miss can briefly retain both the file-read Vec and the Arc
        // handed to the network path. Charge both before starting disk I/O.
        let bytes = payload_bytes
            .checked_mul(PAYLOAD_READ_WORKING_SET_FACTOR)
            .ok_or_else(|| {
                BrokerError::InvalidRecord("delivery working set overflows usize".into())
            })?;
        if bytes > self.capacity {
            return Err(BrokerError::InvalidRecord(format!(
                "delivery working set {bytes} exceeds configured byte budget {}",
                self.capacity
            )));
        }
        let permits = u32::try_from(bytes).map_err(|_| {
            BrokerError::InvalidRecord("delivery batch exceeds the byte budget contract".into())
        })?;
        let permit = match Arc::clone(&self.permits).try_acquire_many_owned(permits) {
            Ok(permit) => permit,
            Err(_) => {
                self.waits_total.fetch_add(1, Ordering::Relaxed);
                let waiter = Waiter::new(Arc::clone(&self.waiters));
                let result = Arc::clone(&self.permits)
                    .acquire_many_owned(permits)
                    .await
                    .map_err(|_| BrokerError::StorageUnavailable);
                drop(waiter);
                result?
            }
        };
        self.in_flight.fetch_add(bytes as u64, Ordering::AcqRel);
        Ok(DeliveryHold {
            _permit: Some(permit),
            bytes: bytes as u64,
            in_flight: Arc::clone(&self.in_flight),
        })
    }

    pub(crate) fn snapshot(&self) -> crate::model::DeliveryBudgetStats {
        crate::model::DeliveryBudgetStats {
            in_flight_bytes: self.in_flight.load(Ordering::Acquire),
            waiters: self.waiters.load(Ordering::Acquire) as u64,
            waits_total: self.waits_total.load(Ordering::Relaxed),
        }
    }
}

impl Drop for DeliveryHold {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

struct Waiter(Arc<AtomicUsize>);

impl Waiter {
    fn new(waiters: Arc<AtomicUsize>) -> Self {
        waiters.fetch_add(1, Ordering::AcqRel);
        Self(waiters)
    }
}

impl Drop for Waiter {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn oversized_request_fails_instead_of_waiting_forever() {
        let budget = DeliveryBudget::new(8);
        let error = match budget.acquire(5).await {
            Ok(_) => panic!("oversized delivery request was accepted"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("exceeds configured byte budget"));
        assert_eq!(budget.snapshot().waiters, 0);
    }
}
