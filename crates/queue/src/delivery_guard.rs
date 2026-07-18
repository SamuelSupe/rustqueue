use crate::delivery_budget::DeliveryHold;
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
        if let Some(index) = self
            .reservations
            .iter()
            .position(|reservation| reservation.id == id)
        {
            self.reservations.swap_remove(index);
        }
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
