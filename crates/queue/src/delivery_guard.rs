use crate::delivery_budget::{DeliveryHold, DeliveryLease};
use crate::model::ReservedDelivery;
use crate::topic::TopicHandle;
use std::sync::Arc;

pub struct DeliveryGuard {
    handle: Option<Arc<TopicHandle>>,
    channel: String,
    reservations: Vec<ReservedDelivery>,
    _hold: Option<DeliveryHold>,
}

impl DeliveryGuard {
    pub(crate) fn new(
        handle: Arc<TopicHandle>,
        channel: String,
        reservations: Vec<ReservedDelivery>,
        hold: DeliveryHold,
    ) -> Self {
        Self {
            handle: Some(handle),
            channel,
            reservations,
            _hold: Some(hold),
        }
    }

    pub(crate) fn empty() -> Self {
        Self {
            handle: None,
            channel: String::new(),
            reservations: Vec::new(),
            _hold: None,
        }
    }

    pub fn accept(&mut self, id: u64) {
        let _ = self.accept_with_token(id);
    }

    pub fn accept_with_token(&mut self, id: u64) -> Option<u64> {
        self.accept_with_lease(id).map(|(token, _lease)| token)
    }

    /// Transfers the consumer's metadata hold together with the delivery token.
    /// Keep the lease until FIN/REQ completion, timeout, or disconnect cleanup.
    pub fn accept_with_lease(&mut self, id: u64) -> Option<(u64, DeliveryLease)> {
        if let Some(index) = self
            .reservations
            .iter()
            .position(|reservation| reservation.id == id)
        {
            let reservation = self.reservations.swap_remove(index);
            return Some((reservation.token, reservation.lease));
        }
        None
    }

    pub fn token(&self, id: u64) -> Option<u64> {
        self.reservations
            .iter()
            .find(|reservation| reservation.id == id)
            .map(|reservation| reservation.token)
    }

    pub fn accept_all(&mut self) {
        self.reservations.clear();
    }
}

impl Default for DeliveryGuard {
    fn default() -> Self {
        Self::empty()
    }
}

impl Drop for DeliveryGuard {
    fn drop(&mut self) {
        if self.reservations.is_empty() {
            return;
        }
        if let Some(handle) = &self.handle {
            handle
                .state
                .lock()
                .cancel(&self.channel, &self.reservations);
            handle.signal();
        }
    }
}
