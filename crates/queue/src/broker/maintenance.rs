use super::*;
use crate::ProtectiveEviction;
use std::collections::{BTreeSet, HashMap};

impl Broker {
    pub async fn compact(&self) -> Result<usize, BrokerError> {
        self.compact_limit(None).await
    }

    pub async fn compact_some(&self, max_topics: usize) -> Result<usize, BrokerError> {
        self.compact_limit(Some(max_topics.max(1))).await
    }

    async fn compact_limit(&self, limit: Option<usize>) -> Result<usize, BrokerError> {
        let _timer = self.inner.metrics.gc.timer();
        let broker = self.clone();
        self.storage_task(move || {
            {
                let _lifecycle = broker.inner.topic_lifecycle.lock();
                broker.cleanup_drained_retired_topics()?;
            }
            let mut outbox_ids = HashMap::<String, BTreeSet<u64>>::new();
            for (source_topic, message_id) in
                crate::outbox::retained_sources(&broker.inner.config.data_path.join("dlq-outbox"))?
            {
                outbox_ids
                    .entry(source_topic)
                    .or_default()
                    .insert(message_id);
            }
            let mut topics: Vec<_> = broker
                .inner
                .topics
                .read()
                .iter()
                .map(|(name, topic)| (name.clone(), Arc::clone(topic)))
                .collect();
            topics.sort_by(|left, right| left.0.cmp(&right.0));
            let selected = limit.map_or(topics.len(), |value| value.min(topics.len()));
            let start = if topics.is_empty() {
                0
            } else {
                broker
                    .inner
                    .gc_cursor
                    .fetch_add(selected, Ordering::Relaxed)
                    % topics.len()
            };
            let mut removed = 0;
            for offset in 0..selected {
                let (_, handle) = &topics[(start + offset) % topics.len()];
                let _commit_gate = handle.commit_gate.lock();
                let mut topic = handle.state.lock();
                let retained = broker.inner.payload_reader.retained_paths();
                let name = topic.name.clone();
                let ids = outbox_ids.get(&name).cloned().unwrap_or_default();
                let deliverable_before = topic.deliverable_position();
                let compacted =
                    topic.compact(broker.inner.config.bootstrap_retention, &retained, &ids)?;
                let visibility_advanced = topic.deliverable_position() > deliverable_before;
                drop(topic);
                if visibility_advanced {
                    handle.signal();
                }
                removed += compacted;
            }
            broker.inner.payload_reader.prune_deleted_files();
            Ok(removed)
        })
        .await
    }

    pub async fn protective_evict_oldest(&self) -> Result<Option<ProtectiveEviction>, BrokerError> {
        let _timer = self.inner.metrics.gc.timer();
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
            let _commit_gate = topic.commit_gate.lock();
            let mut state = topic.state.lock();
            let retained = broker.inner.payload_reader.retained_paths();
            let deliverable_before = state.deliverable_position();
            let result = state
                .protective_evict_oldest(&broker.inner.config.data_path.join("audit"), &retained)?;
            let visibility_advanced = state.deliverable_position() > deliverable_before;
            drop(state);
            if visibility_advanced {
                topic.signal();
            }
            broker.inner.payload_reader.prune_deleted_files();
            Ok(result)
        })
        .await
    }

    pub async fn scrub(&self) -> Result<usize, BrokerError> {
        let _timer = self.inner.metrics.scrub.timer();
        let broker = self.clone();
        self.storage_task(move || {
            let topics: Vec<_> = broker.inner.topics.read().values().cloned().collect();
            let mut count = 0usize;
            for topic in topics {
                let (targets, _lease) = {
                    let topic = topic.state.lock();
                    let targets = topic.scrub_targets()?;
                    let lease = broker
                        .inner
                        .payload_reader
                        .retain_paths(targets.iter().map(|target| target.path.clone()).collect());
                    (targets, lease)
                };
                for target in targets {
                    count += rustqueue_storage::SegmentLog::scrub_target(
                        &target,
                        broker.inner.config.scrub_bytes_per_second,
                    )?;
                }
            }
            Ok(count)
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

    pub async fn expire_in_flight(&self) -> Result<usize, BrokerError> {
        self.ensure_storage_healthy()?;
        if !self
            .inner
            .topics
            .read()
            .values()
            .any(|topic| topic.state.lock().has_expired_in_flight())
        {
            return Ok(0);
        }
        let broker = self.clone();
        self.storage_task(move || {
            broker
                .inner
                .topics
                .read()
                .values()
                .try_fold(0usize, |total, topic| {
                    Ok(total.saturating_add(topic.state.lock().expire_in_flight()?))
                })
        })
        .await
    }

    pub async fn expire_channel_in_flight(
        &self,
        topic: &str,
        channel: &str,
    ) -> Result<usize, BrokerError> {
        self.ensure_storage_healthy()?;
        if !self
            .topic(topic)?
            .state
            .lock()
            .channel_has_expired_in_flight(channel)?
        {
            return Ok(0);
        }
        let broker = self.clone();
        let topic = topic.to_owned();
        let channel = channel.to_owned();
        self.storage_task(move || {
            broker
                .topic(&topic)?
                .state
                .lock()
                .expire_channel_in_flight(&channel)
        })
        .await
    }

    pub async fn flush(&self) -> Result<(), BrokerError> {
        let broker = self.clone();
        self.storage_task(move || {
            for topic in broker.inner.topics.read().values() {
                let _commit_gate = topic.commit_gate.lock();
                topic.state.lock().sync()?;
                topic.signal();
            }
            Ok(())
        })
        .await
    }

    #[doc(hidden)]
    pub async fn checkpoint(&self) -> Result<(), BrokerError> {
        let broker = self.clone();
        self.storage_task(move || {
            for topic in broker.inner.topics.read().values() {
                topic.state.lock().checkpoint_channels()?;
            }
            Ok(())
        })
        .await
    }
}
