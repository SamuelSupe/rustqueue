use super::*;

impl Partition {
    pub(super) fn apply(
        &mut self,
        record: Record,
        location: &rustqueue_storage::RecordLocation,
    ) -> Result<(), BrokerError> {
        match record.kind {
            RecordKind::PublishBatch => {
                let (operation_id, payload) = if record.flags & 1 == 1 {
                    if record.payload.len() < 8 {
                        return Err(BrokerError::InvalidRecord(
                            "replicated publish is missing its operation ID".into(),
                        ));
                    }
                    (
                        Some(u64::from_be_bytes(record.payload[..8].try_into().unwrap())),
                        &record.payload[8..],
                    )
                } else {
                    (None, record.payload.as_slice())
                };
                let prefix = if operation_id.is_some() { 8 } else { 0 };
                for message in batch::decode_refs(
                    payload,
                    record.index,
                    &location.segment,
                    location.offset + rustqueue_storage::HEADER_LEN as u64 + prefix,
                )? {
                    if (message.id >> 48) as u16 != self.slot {
                        return Err(BrokerError::InvalidRecord(format!(
                            "message {} belongs to a different partition slot",
                            message.id
                        )));
                    }
                    self.next_sequence = self
                        .next_sequence
                        .max((message.id & ((1u64 << 48) - 1)).saturating_add(1));
                    self.messages.push(message);
                }
                for channel in self.channels.values_mut() {
                    channel.delivery_blocked_until_ms = 0;
                }
            }
            kind => {
                let command: ChannelCommand = serde_json::from_slice(&record.payload)
                    .map_err(|error| BrokerError::InvalidRecord(error.to_string()))?;
                match kind {
                    RecordKind::CreateChannel => {
                        let barrier = self.messages.len();
                        let max_ack_gap = self.max_ack_gap;
                        self.channels
                            .entry(command.channel)
                            .or_insert_with(|| ChannelState::new(barrier, false, max_ack_gap));
                    }
                    RecordKind::DeleteChannel => {
                        self.channels.remove(&command.channel);
                    }
                    RecordKind::Ack => {
                        let position = self.message_position(command.message_id);
                        if let (Some(channel), Some(position)) =
                            (self.channels.get_mut(&command.channel), position)
                        {
                            channel.acknowledge(position, &self.messages);
                            channel.requeued_until.remove(&command.message_id);
                            channel.attempts.remove(&command.message_id);
                        }
                    }
                    RecordKind::Requeue => {
                        if let Some(channel) = self.channels.get_mut(&command.channel) {
                            channel
                                .requeued_until
                                .insert(command.message_id, command.available_at_ms);
                            channel.delivery_blocked_until_ms = 0;
                        }
                    }
                    RecordKind::PauseChannel => {
                        if let Some(channel) = self.channels.get_mut(&command.channel) {
                            channel.paused = command.paused;
                            channel.delivery_blocked_until_ms = 0;
                        }
                    }
                    RecordKind::EmptyChannel => {
                        if let Some(channel) = self.channels.get_mut(&command.channel) {
                            channel.empty_through(self.messages.len());
                            channel.in_flight.clear();
                            channel.requeued_until.clear();
                            channel.attempts.clear();
                        }
                    }
                    RecordKind::Membership | RecordKind::Noop | RecordKind::PublishBatch => {}
                }
            }
        }
        Ok(())
    }

    pub(super) fn stats(&self) -> PartitionStats {
        let now = now_ms();
        let mut channels: Vec<_> = self
            .channels
            .iter()
            .map(|(name, state)| {
                let start = state.ack_floor.max(state.barrier).min(self.messages.len());
                let depth = state.depth(self.messages.len()) as u64;
                let deferred_count = self.messages[start..]
                    .iter()
                    .enumerate()
                    .filter(|(offset, message)| {
                        !state.is_acknowledged(start + offset, message.id)
                            && state
                                .requeued_until
                                .get(&message.id)
                                .copied()
                                .unwrap_or(message.available_at_ms)
                                > now
                    })
                    .count() as u64;
                ChannelStats {
                    name: name.clone(),
                    depth,
                    in_flight_count: state.in_flight.len() as u64,
                    deferred_count,
                    paused: state.paused,
                    ephemeral: state.ephemeral,
                    ack_cursor: state.ack_floor as u64,
                    ack_gap: state.ack_gap() as u64,
                }
            })
            .collect();
        channels.sort_by(|left, right| left.name.cmp(&right.name));
        PartitionStats {
            partition: self.number,
            slot: self.slot,
            message_count: self.messages.len() as u64,
            log_records: self.log.last_index().unwrap_or(0),
            channels,
        }
    }
}
