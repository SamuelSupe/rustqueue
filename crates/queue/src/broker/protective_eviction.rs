use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProtectiveEvictionCandidate {
    pub through_message_id: u64,
    pub message_count: usize,
    pub payload_bytes: u64,
}

impl Partition {
    pub(super) fn drop_prefix(&mut self, count: usize) -> usize {
        let count = count.min(self.messages.len());
        if count == 0 {
            return 0;
        }
        let retained_sequence = self.base_sequence.saturating_add(count as u64);
        let slot = self.slot;
        self.messages.drain(..count);
        self.base_sequence = self.messages.first().map_or(self.next_sequence, |message| {
            message.id & ((1u64 << 48) - 1)
        });
        for channel in self.channels.values_mut() {
            channel.barrier = channel.barrier.saturating_sub(count);
            channel.cursor = channel.cursor.saturating_sub(count);
            channel.retention_cursor = channel.retention_cursor.saturating_sub(count);
            channel.ack_floor = channel.ack_floor.saturating_sub(count);
            channel
                .acknowledged
                .retain(|id| retained_message(*id, slot, retained_sequence));
            channel
                .in_flight
                .retain(|id, _| retained_message(*id, slot, retained_sequence));
            channel
                .loading
                .retain(|id, _| retained_message(*id, slot, retained_sequence));
            channel
                .requeued_until
                .retain(|id, _| retained_message(*id, slot, retained_sequence));
            channel
                .attempts
                .retain(|id, _| retained_message(*id, slot, retained_sequence));
            channel.delivery_blocked_until_ms = 0;
        }
        self.signal_delivery();
        count
    }
}

impl Broker {
    /// Selects every oldest message that references the same payload segment.
    /// The caller must snapshot after applying the replicated cutoff; that seal
    /// is what makes physical deletion safe even when this is the last segment.
    pub fn protective_eviction_candidate(
        &self,
        topic: &str,
        partition: u16,
    ) -> Result<Option<ProtectiveEvictionCandidate>, BrokerError> {
        let partition = self.partition(topic, partition)?;
        let state = partition.lock();
        let Some(first) = state.messages.first() else {
            return Ok(None);
        };
        let path = first.payload.path.as_ref();
        let count = state
            .messages
            .iter()
            .take_while(|message| message.payload.path.as_ref() == path)
            .count();
        let payload_bytes = state.messages[..count]
            .iter()
            .map(|message| message.payload.len as u64)
            .sum();
        Ok(Some(ProtectiveEvictionCandidate {
            through_message_id: state.messages[count - 1].id,
            message_count: count,
            payload_bytes,
        }))
    }

    /// Applies a quorum-replicated logical eviction. Physical files remain
    /// referenced until the caller completes a snapshot and Raft log purge.
    pub fn protective_evict_through(
        &self,
        topic: &str,
        partition: u16,
        through_message_id: u64,
    ) -> Result<usize, BrokerError> {
        if !self.config.projection_only {
            return Err(BrokerError::InvalidRecord(
                "protective eviction requires a replicated projection".into(),
            ));
        }
        let partition = self.partition(topic, partition)?;
        let mut state = partition.lock();
        if (through_message_id >> 48) as u16 != state.slot {
            return Err(BrokerError::InvalidRecord(
                "protective eviction message belongs to another partition".into(),
            ));
        }
        let sequence = through_message_id & ((1u64 << 48) - 1);
        if sequence < state.base_sequence {
            return Ok(0);
        }
        let count = sequence
            .checked_sub(state.base_sequence)
            .and_then(|offset| usize::try_from(offset).ok())
            .and_then(|offset| offset.checked_add(1))
            .ok_or_else(|| BrokerError::InvalidRecord("invalid eviction boundary".into()))?;
        if count > state.messages.len() || state.messages[count - 1].id != through_message_id {
            return Err(BrokerError::InvalidRecord(
                "protective eviction boundary is not present".into(),
            ));
        }
        Ok(state.drop_prefix(count))
    }
}

fn retained_message(message_id: u64, slot: u16, retained_sequence: u64) -> bool {
    (message_id >> 48) as u16 == slot && (message_id & ((1u64 << 48) - 1)) >= retained_sequence
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustqueue_storage::PayloadRef;
    use tempfile::tempdir;

    fn payload(path: &str, len: u32) -> PayloadRef {
        PayloadRef {
            path: Arc::new(PathBuf::from(path)),
            offset: 0,
            len,
            crc32c: 0,
        }
    }

    #[test]
    fn evicts_only_a_complete_oldest_segment_and_is_idempotent() {
        let directory = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig {
            data_path: directory.path().to_path_buf(),
            projection_only: true,
            max_message_bytes: 1024,
            ..BrokerConfig::default()
        })
        .unwrap();
        broker
            .ensure_topic_layout("events", &[(0, 7)], &[7])
            .unwrap();
        broker
            .create_channel_partition("events", "workers", 0)
            .unwrap();
        let first = broker
            .publish_replicated_refs(
                1,
                "events",
                vec![payload("segment-a", 10), payload("segment-a", 20)],
                0,
                0,
                0,
                1,
            )
            .unwrap();
        broker
            .publish_replicated_refs(2, "events", vec![payload("segment-b", 30)], 0, 0, 0, 2)
            .unwrap();

        let candidate = broker
            .protective_eviction_candidate("events", 0)
            .unwrap()
            .unwrap();
        assert_eq!(candidate.through_message_id, first[1]);
        assert_eq!(candidate.message_count, 2);
        assert_eq!(candidate.payload_bytes, 30);
        assert_eq!(
            broker
                .protective_evict_through("events", 0, candidate.through_message_id)
                .unwrap(),
            2
        );
        assert_eq!(
            broker.partition_stats("events", 0).unwrap().message_count,
            1
        );
        assert_eq!(
            broker
                .protective_evict_through("events", 0, candidate.through_message_id)
                .unwrap(),
            0
        );
        let final_segment = broker
            .protective_eviction_candidate("events", 0)
            .unwrap()
            .unwrap();
        assert_eq!(final_segment.message_count, 1);
    }
}
