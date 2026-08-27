use super::*;
use crate::model::{TopicPolicy, TtlDiscardReport};
use std::sync::atomic::Ordering;

const TTL_DISCARD_MAX_MESSAGES_PER_BATCH: u64 = 65_536;

impl ExpirationSchedule {
    pub(super) fn update(&mut self, topic: &str, deadline_ns: Option<i64>) {
        if let Some(previous) = self.by_topic.remove(topic) {
            if let Some(topics) = self.by_deadline.get_mut(&previous) {
                topics.remove(topic);
                if topics.is_empty() {
                    self.by_deadline.remove(&previous);
                }
            }
        }
        if let Some(deadline_ns) = deadline_ns {
            self.by_topic.insert(topic.to_owned(), deadline_ns);
            self.by_deadline
                .entry(deadline_ns)
                .or_default()
                .insert(topic.to_owned());
        }
    }

    fn contains(&self, topic: &str) -> bool {
        self.by_topic.contains_key(topic)
    }

    fn is_due(&self, topic: &str, now_ns: i64) -> bool {
        self.by_topic
            .get(topic)
            .is_some_and(|deadline_ns| *deadline_ns <= now_ns)
    }

    fn take_due(&mut self, now_ns: i64, limit: usize) -> Vec<String> {
        let mut due = Vec::new();
        while due.len() < limit {
            let Some((&deadline, _)) = self.by_deadline.first_key_value() else {
                break;
            };
            if deadline > now_ns {
                break;
            }
            let Some(mut topics) = self.by_deadline.remove(&deadline) else {
                continue;
            };
            while due.len() < limit {
                let Some(topic) = topics.pop_first() else {
                    break;
                };
                self.by_topic.remove(&topic);
                due.push(topic);
            }
            if !topics.is_empty() {
                for topic in &topics {
                    self.by_topic.insert(topic.clone(), deadline);
                }
                self.by_deadline.insert(deadline, topics);
            }
        }
        due
    }
}

impl Broker {
    pub(super) fn rebuild_expiration_schedule(&self) -> Result<(), BrokerError> {
        let topics: Vec<_> = self
            .inner
            .topics
            .read()
            .iter()
            .map(|(name, handle)| (name.clone(), Arc::clone(handle)))
            .collect();
        let mut schedule = self.inner.expiration_schedule.lock();
        *schedule = ExpirationSchedule::default();
        for (name, handle) in topics {
            let deadline = handle.state.lock().next_expiration_ns()?;
            schedule.update(&name, deadline);
        }
        Ok(())
    }

    pub(super) fn refresh_expiration_schedule_locked(
        &self,
        topic: &str,
        state: &crate::topic::Topic,
    ) -> Result<(), BrokerError> {
        let mut schedule = self.inner.expiration_schedule.lock();
        if schedule.contains(topic) && state.policy().ttl_seconds().is_some() {
            return Ok(());
        }
        let deadline = state.next_expiration_ns()?;
        schedule.update(topic, deadline);
        Ok(())
    }

    pub(super) fn replace_expiration_schedule_locked(
        &self,
        topic: &str,
        state: &crate::topic::Topic,
    ) -> Result<(), BrokerError> {
        let deadline = state.next_expiration_ns()?;
        self.inner
            .expiration_schedule
            .lock()
            .update(topic, deadline);
        Ok(())
    }

    pub fn topic_policy(&self, topic: &str) -> Result<TopicPolicy, BrokerError> {
        Ok(self.topic(topic)?.state.lock().policy())
    }

    pub fn delivery_expiration_ns(
        &self,
        topic: &str,
        timestamp_ns: i64,
    ) -> Result<Option<i64>, BrokerError> {
        Ok(self
            .topic(topic)?
            .state
            .lock()
            .expiration_ns_for_timestamp(timestamp_ns))
    }

    pub fn subscribe_topic_policy(
        &self,
        topic: &str,
    ) -> Result<tokio::sync::watch::Receiver<TopicPolicy>, BrokerError> {
        Ok(self.topic(topic)?.subscribe_policy())
    }

    pub fn topic_reliable_policy_epoch(&self, topic: &str) -> Result<u64, BrokerError> {
        Ok(self.topic(topic)?.reliable_policy_epoch())
    }

    pub async fn configure_topic_policy(
        &self,
        topic: &str,
        policy: TopicPolicy,
    ) -> Result<bool, BrokerError> {
        let policy = policy
            .validate()
            .map_err(|error| BrokerError::InvalidTopicPolicy(error.into()))?;
        let _outbox_guard = self.inner.outbox_moves.lock().await;
        let broker = self.clone();
        let topic = topic.to_owned();
        let topic_for_update = topic.clone();
        let changed = self
            .storage_task(move || {
                let handle = broker.topic(&topic_for_update)?;
                let _commit_gate = handle.commit_gate.lock();
                let _channel_commit_gate = handle.channel_commit_gate.lock();
                let mut state = handle.state.lock();
                let changed = handle.set_policy(&mut state, policy)?;
                broker.replace_expiration_schedule_locked(&topic_for_update, &state)?;
                Ok(changed)
            })
            .await?;
        if policy.delivery_mode == crate::model::DeliveryMode::TtlDiscard {
            let directory = self.inner.config.data_path.join("dlq-outbox");
            let source_topic = topic.clone();
            self.storage_task(move || {
                crate::outbox::remove_source_topic(&directory, &source_topic)
            })
            .await?;
        }
        if changed {
            let _ = self.expire_topic_if_due(&topic).await?;
        }
        Ok(changed)
    }

    pub async fn expire_due_topics(
        &self,
        max_topics: usize,
    ) -> Result<Vec<TtlDiscardReport>, BrokerError> {
        let due = self
            .inner
            .expiration_schedule
            .lock()
            .take_due(now_ns(), max_topics.max(1));
        let mut reports = Vec::new();
        for topic in due {
            if let Some(report) = self.expire_topic_at(&topic, now_ns()).await? {
                reports.push(report);
            }
            tokio::task::yield_now().await;
        }
        Ok(reports)
    }

    pub async fn expire_topic_if_due(
        &self,
        topic: &str,
    ) -> Result<Option<TtlDiscardReport>, BrokerError> {
        let now_ns = now_ns();
        if !self.inner.expiration_schedule.lock().is_due(topic, now_ns) {
            return Ok(None);
        }
        self.expire_topic_at(topic, now_ns).await
    }

    async fn expire_topic_at(
        &self,
        topic: &str,
        now_ns: i64,
    ) -> Result<Option<TtlDiscardReport>, BrokerError> {
        let broker = self.clone();
        let topic_name = topic.to_owned();
        self.storage_task(move || {
            let handle = match broker.topic(&topic_name) {
                Ok(handle) => handle,
                Err(BrokerError::TopicNotFound) => {
                    broker
                        .inner
                        .expiration_schedule
                        .lock()
                        .update(&topic_name, None);
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };
            let _commit_gate = handle.commit_gate.lock();
            let _channel_commit_gate = handle.channel_commit_gate.lock();
            let mut state = handle.state.lock();
            let Some(prepared) =
                state.prepare_ttl_discard(now_ns, TTL_DISCARD_MAX_MESSAGES_PER_BATCH)?
            else {
                broker.replace_expiration_schedule_locked(&topic_name, &state)?;
                return Ok(None);
            };
            rustqueue_storage::crash_failpoint("ttl_discard_after_wal_append_before_fsync");
            let syncs = state.prepare_channel_wal_syncs(prepared.channels.iter())?;
            drop(state);
            let sync_result = syncs
                .into_iter()
                .try_for_each(|wal| wal.sync_data().map_err(BrokerError::from));
            if let Err(error) = sync_result {
                handle
                    .state
                    .lock()
                    .mark_channel_wal_sync_failed(prepared.channels.iter());
                return broker.observe_storage_result(Err(error));
            }
            rustqueue_storage::crash_failpoint("ttl_discard_after_wal_fsync_before_manifest");
            let mut state = handle.state.lock();
            state.checkpoint_channels_if_needed(prepared.channels.iter())?;
            state.commit_ttl_discard(prepared.through_position, prepared.messages)?;
            rustqueue_storage::crash_failpoint("ttl_discard_after_manifest_before_metric");
            broker.replace_expiration_schedule_locked(&topic_name, &state)?;
            broker
                .inner
                .metrics
                .ttl_discarded_messages
                .fetch_add(prepared.messages, Ordering::Relaxed);
            let report = TtlDiscardReport {
                topic: topic_name,
                through_position: prepared.through_position,
                messages: prepared.messages,
            };
            drop(state);
            handle.signal();
            Ok(Some(report))
        })
        .await
    }
}
