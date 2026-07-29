use super::*;
use crate::topic::index::MetadataReservation;

impl Broker {
    pub(super) fn reserve_message_metadata(
        &self,
        messages: usize,
    ) -> Result<MetadataReservation, BrokerError> {
        if let Some(reservation) = self.inner.message_index_cache.try_reserve(messages) {
            return Ok(reservation);
        }
        let _spill = self.inner.metadata_spill.lock();
        loop {
            let observed = self.inner.message_index_cache.change_epoch();
            if let Some(reservation) = self.inner.message_index_cache.try_reserve(messages) {
                return Ok(reservation);
            }
            let mut topics: Vec<_> = self.inner.topics.read().values().cloned().collect();
            topics
                .sort_by_key(|topic| std::cmp::Reverse(topic.state.lock().active_metadata_count()));
            let mut progressed = false;
            for topic in topics {
                let _commit_gate = topic.commit_gate.lock();
                if topic.state.lock().spill_message_metadata()? > 0 {
                    progressed = true;
                }
                if let Some(reservation) = self.inner.message_index_cache.try_reserve(messages) {
                    return Ok(reservation);
                }
            }
            if !progressed {
                self.inner.message_index_cache.wait_for_change(observed);
            }
        }
    }
}
