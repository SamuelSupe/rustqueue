use super::*;
use crate::ProtectiveEviction;
use std::collections::{BTreeSet, HashMap};

impl Broker {
    pub async fn compact(&self) -> Result<usize, BrokerError> {
        let broker = self.clone();
        self.storage_task(move || {
            let mut outbox_ids = HashMap::<String, BTreeSet<u64>>::new();
            for (_, entry) in
                crate::outbox::load_all(&broker.inner.config.data_path.join("dlq-outbox"))?
            {
                outbox_ids
                    .entry(entry.source_topic)
                    .or_default()
                    .insert(entry.message_id);
            }
            let mut removed = 0;
            for topic in broker.inner.topics.read().values() {
                let mut topic = topic.state.lock();
                let retained = broker.inner.payload_reader.retained_paths();
                let name = topic.name.clone();
                let ids = outbox_ids.get(&name).cloned().unwrap_or_default();
                removed +=
                    topic.compact(broker.inner.config.bootstrap_retention, &retained, &ids)?;
            }
            Ok(removed)
        })
        .await
    }

    pub async fn protective_evict_oldest(&self) -> Result<Option<ProtectiveEviction>, BrokerError> {
        let broker = self.clone();
        self.storage_task(move || {
            let topics: Vec<_> = broker.inner.topics.read().values().cloned().collect();
            let candidate = topics
                .into_iter()
                .filter_map(|topic| {
                    let timestamp = topic.state.lock().oldest_message_timestamp()?;
                    Some((timestamp, topic))
                })
                .min_by_key(|(timestamp, _)| *timestamp);
            let Some((_, topic)) = candidate else {
                return Ok(None);
            };
            let mut topic = topic.state.lock();
            let retained = broker.inner.payload_reader.retained_paths();
            topic.protective_evict_oldest(&broker.inner.config.data_path.join("audit"), &retained)
        })
        .await
    }

    pub async fn scrub(&self) -> Result<usize, BrokerError> {
        let broker = self.clone();
        self.storage_task(move || {
            broker
                .inner
                .topics
                .read()
                .values()
                .try_fold(0usize, |count, topic| {
                    Ok(count + topic.state.lock().scrub()?)
                })
        })
        .await
    }

    pub fn release_all_in_flight(&self) -> usize {
        self.inner
            .topics
            .read()
            .values()
            .map(|topic| topic.state.lock().release_all())
            .sum()
    }

    pub async fn flush(&self) -> Result<(), BrokerError> {
        let broker = self.clone();
        self.storage_task(move || {
            for topic in broker.inner.topics.read().values() {
                topic.state.lock().sync()?;
            }
            Ok(())
        })
        .await
    }
}
