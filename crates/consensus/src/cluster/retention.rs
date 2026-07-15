use super::*;
use crate::{PartitionLifecycle, TopicState};

#[derive(Clone, Debug)]
pub struct RetentionOptions {
    pub message_retention_seconds: u64,
    pub dead_letter_suffix: String,
    pub max_groups_per_cycle: usize,
}

impl Default for RetentionOptions {
    fn default() -> Self {
        Self {
            message_retention_seconds: 0,
            dead_letter_suffix: ".DLQ".into(),
            max_groups_per_cycle: 32,
        }
    }
}

impl ClusterRuntime {
    pub(super) async fn reconcile_retention(&self) -> anyhow::Result<usize> {
        if self.retention.message_retention_seconds == 0 || self.retention.max_groups_per_cycle == 0
        {
            return Ok(0);
        }
        let snapshot = self.metadata.snapshot();
        let mut candidates = Vec::new();
        for topic in snapshot.topics.into_values() {
            if topic.state != TopicState::Active
                || topic.paused
                || topic.name.ends_with(&self.retention.dead_letter_suffix)
            {
                continue;
            }
            for partition in topic
                .partitions
                .iter()
                .filter(|partition| partition.lifecycle == PartitionLifecycle::Active)
            {
                for channel in topic.channels.values().filter(|channel| {
                    channel.state == ChannelLifecycle::Active
                        && !channel.ephemeral
                        && !channel.paused
                }) {
                    candidates.push((topic.name.clone(), channel.name.clone(), partition.clone()));
                }
            }
        }
        if candidates.is_empty() {
            return Ok(0);
        }
        let count = candidates.len().min(self.retention.max_groups_per_cycle);
        let start = self
            .retention_cursor
            .fetch_add(count as u64, Ordering::Relaxed) as usize
            % candidates.len();
        let now_ms = wall_time_ms().min(i64::MAX as u64) as i64;
        let now_ns = now_ms.saturating_mul(1_000_000);
        let retention_ns = self
            .retention
            .message_retention_seconds
            .saturating_mul(1_000_000_000)
            .min(i64::MAX as u64) as i64;
        let expired_before_ns = now_ns.saturating_sub(retention_ns);
        let mut moved = 0;
        for offset in 0..count {
            let (topic, channel, partition) = &candidates[(start + offset) % candidates.len()];
            let request = FetchRequest {
                topic: topic.clone(),
                channel: channel.clone(),
                partition_cursor: 0,
                timeout_ms: 30_000,
                max_messages: crate::MAX_FETCH_MESSAGES,
                max_bytes: crate::MAX_FETCH_BYTES,
                wait_ms: 0,
                partition: Some(partition.number),
                expired_before_ns: Some(expired_before_ns),
            };
            match self.fetch_partition(partition, request).await {
                Ok(response) if response.error.is_none() && !response.deliveries.is_empty() => {
                    moved += self
                        .move_expired_batch(topic, channel, response.deliveries, now_ms)
                        .await;
                }
                Ok(_) => {}
                Err(error) => tracing::debug!(
                    %error,
                    source_topic = topic,
                    source_channel = channel,
                    group_id = %partition.global_id(),
                    "retention candidate fetch will retry"
                ),
            }
        }
        Ok(moved)
    }

    async fn move_expired_batch(
        &self,
        topic: &str,
        channel: &str,
        deliveries: Vec<crate::RemoteDelivery>,
        now_ms: i64,
    ) -> usize {
        let ids: Vec<_> = deliveries.iter().map(|delivery| delivery.id).collect();
        let target = match dead_letter_topic(topic, channel, &self.retention.dead_letter_suffix) {
            Ok(target) => target,
            Err(error) => {
                self.retention_failures.fetch_add(1, Ordering::Relaxed);
                tracing::error!(%error, source_topic = topic, source_channel = channel);
                self.release_retention_candidates(topic, channel, ids).await;
                return 0;
            }
        };
        let bodies = deliveries
            .into_iter()
            .map(|delivery| delivery.body)
            .collect();
        let operation_id = retention_operation_id(topic, channel, &ids);
        let published = self
            .write(QueueCommand::Publish {
                operation_id,
                topic: target.clone(),
                bodies,
                timestamp_ns: now_ms.saturating_mul(1_000_000),
                available_at_ms: now_ms,
                partition: None,
                routing_key: None,
            })
            .await
            .and_then(|response| ensure_response(&response));
        if let Err(error) = published {
            self.retention_failures.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(%error, source_topic = topic, source_channel = channel, "DLQ publish will retry");
            self.release_retention_candidates(topic, channel, ids).await;
            return 0;
        }

        let acknowledgements = self
            .write_ack_batch(
                ids.iter()
                    .map(|id| QueueCommand::Finish {
                        topic: topic.to_owned(),
                        channel: channel.to_owned(),
                        message_id: *id,
                    })
                    .collect(),
            )
            .await;
        let failed: Vec<_> = acknowledgements
            .iter()
            .filter_map(|result| result.error.as_ref().map(|_| result.message_id))
            .collect();
        if !failed.is_empty() {
            self.retention_failures
                .fetch_add(failed.len() as u64, Ordering::Relaxed);
            self.release_retention_candidates(topic, channel, failed)
                .await;
        }
        let completed = acknowledgements.len().saturating_sub(
            acknowledgements
                .iter()
                .filter(|result| result.error.is_some())
                .count(),
        );
        self.retention_moved
            .fetch_add(completed as u64, Ordering::Relaxed);
        tracing::info!(
            source_topic = topic,
            source_channel = channel,
            target_topic = target,
            attempted = ids.len(),
            completed,
            "retention batch moved to dead-letter topic"
        );
        completed
    }

    async fn release_retention_candidates(&self, topic: &str, channel: &str, ids: Vec<u64>) {
        if ids.is_empty() {
            return;
        }
        if let Err(error) = self
            .release(ReleaseRequest {
                topic: topic.to_owned(),
                channel: channel.to_owned(),
                message_ids: ids,
            })
            .await
        {
            tracing::debug!(%error, "failed to release retention candidates");
        }
    }
}

pub fn dead_letter_topic(topic: &str, channel: &str, suffix: &str) -> Result<String, String> {
    if suffix.is_empty()
        || suffix.len() > 16
        || !suffix
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err("dead-letter suffix is invalid".into());
    }
    let candidate = format!("{topic}.{channel}{suffix}");
    if rustqueue_protocol::validate_name(&candidate).is_ok() {
        return Ok(candidate);
    }
    let hash = crc32c::crc32c(format!("{topic}\0{channel}").as_bytes());
    let tail = format!(".{hash:08x}{suffix}");
    let keep = 64usize.saturating_sub(tail.len()).min(topic.len());
    let candidate = format!("{}{}", &topic[..keep], tail);
    rustqueue_protocol::validate_name(&candidate)
        .map_err(|_| "cannot derive a valid dead-letter topic name".to_owned())?;
    Ok(candidate)
}

fn retention_operation_id(topic: &str, channel: &str, ids: &[u64]) -> u64 {
    let hash = crc32c::crc32c(format!("{topic}\0{channel}").as_bytes()) as u64;
    ids.first().copied().unwrap_or_default()
        ^ ids.last().copied().unwrap_or_default().rotate_left(17)
        ^ hash.rotate_left(32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn long_dlq_names_are_stable_and_nsq_safe() {
        let topic = "a".repeat(64);
        let channel = "b".repeat(64);
        let name = dead_letter_topic(&topic, &channel, ".DLQ").unwrap();
        assert!(name.len() <= 64);
        assert!(rustqueue_protocol::validate_name(&name).is_ok());
        assert_eq!(name, dead_letter_topic(&topic, &channel, ".DLQ").unwrap());
    }
}
