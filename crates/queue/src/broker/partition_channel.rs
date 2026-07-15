use super::*;

impl Partition {
    pub(super) fn message_position(&self, id: u64) -> Option<usize> {
        if (id >> 48) as u16 != self.slot {
            return None;
        }
        let sequence = id & ((1u64 << 48) - 1);
        let position = usize::try_from(sequence.checked_sub(self.base_sequence)?).ok()?;
        self.messages
            .get(position)
            .is_some_and(|message| message.id == id)
            .then_some(position)
    }

    pub(super) fn create_channel(&mut self, name: &str) -> Result<(), BrokerError> {
        if self.channels.contains_key(name) {
            return Ok(());
        }
        let ephemeral = name.ends_with("#ephemeral");
        let barrier = self.messages.len();
        if !ephemeral {
            self.append_channel_command(
                RecordKind::CreateChannel,
                &ChannelCommand {
                    channel: name.to_owned(),
                    message_id: 0,
                    available_at_ms: 0,
                    paused: false,
                },
            )?;
        }
        self.channels.insert(
            name.to_owned(),
            ChannelState::new(barrier, ephemeral, self.max_ack_gap),
        );
        Ok(())
    }

    pub(super) fn delete_channel(&mut self, name: &str) -> Result<(), BrokerError> {
        let Some(state) = self.channels.get(name) else {
            return Ok(());
        };
        if !state.ephemeral {
            self.append_channel_command(
                RecordKind::DeleteChannel,
                &ChannelCommand {
                    channel: name.to_owned(),
                    message_id: 0,
                    available_at_ms: 0,
                    paused: false,
                },
            )?;
        }
        self.channels.remove(name);
        Ok(())
    }

    pub(super) fn set_channel_paused(
        &mut self,
        name: &str,
        paused: bool,
    ) -> Result<(), BrokerError> {
        let ephemeral = self
            .channels
            .get(name)
            .ok_or(BrokerError::ChannelNotFound)?
            .ephemeral;
        if !ephemeral {
            self.append_channel_command(
                RecordKind::PauseChannel,
                &ChannelCommand {
                    channel: name.to_owned(),
                    message_id: 0,
                    available_at_ms: 0,
                    paused,
                },
            )?;
        }
        let state = self.channels.get_mut(name).unwrap();
        state.paused = paused;
        state.delivery_blocked_until_ms = 0;
        self.signal_delivery();
        Ok(())
    }

    pub(super) fn empty_channel(&mut self, name: &str) -> Result<(), BrokerError> {
        let ephemeral = self
            .channels
            .get(name)
            .ok_or(BrokerError::ChannelNotFound)?
            .ephemeral;
        if !ephemeral {
            self.append_channel_command(
                RecordKind::EmptyChannel,
                &ChannelCommand {
                    channel: name.to_owned(),
                    message_id: 0,
                    available_at_ms: 0,
                    paused: false,
                },
            )?;
        }
        let state = self.channels.get_mut(name).unwrap();
        state.empty_through(self.messages.len());
        state.in_flight.clear();
        state.loading.clear();
        state.requeued_until.clear();
        self.signal_delivery();
        Ok(())
    }
    pub(super) fn has_ready_message(
        &mut self,
        channel: &str,
        now: i64,
    ) -> Result<bool, BrokerError> {
        let state = self
            .channels
            .get_mut(channel)
            .ok_or(BrokerError::ChannelNotFound)?;
        if state.paused || self.messages.len() <= state.barrier {
            return Ok(false);
        }
        if state.delivery_blocked_until_ms > now {
            return Ok(false);
        }
        state.in_flight.retain(|_, item| item.deadline_ms > now);
        state.loading.retain(|_, item| item.deadline_ms > now);
        let floor = state.ack_floor.max(state.barrier);
        let ceiling = floor
            .saturating_add(self.max_ack_gap)
            .min(self.messages.len());
        let mut blocked_until = i64::MAX;
        for (position, message) in self.messages.iter().enumerate().take(ceiling).skip(floor) {
            if state.is_acknowledged(position, message.id) {
                continue;
            }
            if let Some(delivery) = state.in_flight.get(&message.id) {
                blocked_until = blocked_until.min(delivery.deadline_ms);
                continue;
            }
            if let Some(delivery) = state.loading.get(&message.id) {
                blocked_until = blocked_until.min(delivery.deadline_ms);
                continue;
            }
            let available_at = state
                .requeued_until
                .get(&message.id)
                .copied()
                .unwrap_or(message.available_at_ms);
            if available_at <= now {
                state.delivery_blocked_until_ms = 0;
                return Ok(true);
            }
            blocked_until = blocked_until.min(available_at);
        }
        state.delivery_blocked_until_ms = blocked_until;
        Ok(false)
    }

    pub(super) fn delivery_blocked_until(&self, channel: &str) -> Result<i64, BrokerError> {
        self.channels
            .get(channel)
            .map(|state| state.delivery_blocked_until_ms)
            .ok_or(BrokerError::ChannelNotFound)
    }

    pub(super) fn reserve_next_message(
        &mut self,
        channel: &str,
        timeout: Duration,
    ) -> Result<Option<ReservedDelivery>, BrokerError> {
        let state = self
            .channels
            .get_mut(channel)
            .ok_or(BrokerError::ChannelNotFound)?;
        if state.paused || self.messages.len() <= state.barrier {
            return Ok(None);
        }
        let now = now_ms();
        if state.delivery_blocked_until_ms > now {
            return Ok(None);
        }
        state.in_flight.retain(|_, item| item.deadline_ms > now);
        state.loading.retain(|_, loading| loading.deadline_ms > now);
        let floor = state.ack_floor.max(state.barrier);
        let ceiling = floor
            .saturating_add(self.max_ack_gap)
            .min(self.messages.len());
        if state.cursor < floor || state.cursor >= ceiling {
            state.cursor = floor;
        }
        let available_count = ceiling.saturating_sub(floor);
        let mut blocked_until = i64::MAX;
        for _ in 0..available_count {
            if state.cursor < floor || state.cursor >= ceiling {
                state.cursor = floor;
            }
            let position = state.cursor;
            let message = &self.messages[position];
            state.cursor += 1;
            if state.is_acknowledged(position, message.id) {
                continue;
            }
            if let Some(delivery) = state.in_flight.get(&message.id) {
                blocked_until = blocked_until.min(delivery.deadline_ms);
                continue;
            }
            if let Some(delivery) = state.loading.get(&message.id) {
                blocked_until = blocked_until.min(delivery.deadline_ms);
                continue;
            }
            let available_at = state
                .requeued_until
                .get(&message.id)
                .copied()
                .unwrap_or(message.available_at_ms);
            if available_at > now {
                blocked_until = blocked_until.min(available_at);
                continue;
            }
            let token = self.next_delivery_token;
            self.next_delivery_token = self.next_delivery_token.wrapping_add(1).max(1);
            let timeout_ms = duration_ms(timeout);
            state.loading.insert(
                message.id,
                LoadingDelivery {
                    token,
                    deadline_ms: now.saturating_add(timeout_ms.max(30_000)),
                },
            );
            state.delivery_blocked_until_ms = 0;
            return Ok(Some(ReservedDelivery {
                message_id: message.id,
                timestamp_ns: message.timestamp_ns,
                payload: message.payload.clone(),
                token,
                timeout_ms,
            }));
        }
        state.delivery_blocked_until_ms = blocked_until;
        Ok(None)
    }

    pub(super) fn reserve_expired_message(
        &mut self,
        channel: &str,
        expired_before_ns: i64,
        timeout: Duration,
    ) -> Result<Option<ReservedDelivery>, BrokerError> {
        let state = self
            .channels
            .get_mut(channel)
            .ok_or(BrokerError::ChannelNotFound)?;
        if state.paused || state.ephemeral || self.messages.len() <= state.barrier {
            return Ok(None);
        }
        let now = now_ms();
        state.in_flight.retain(|_, item| item.deadline_ms > now);
        state.loading.retain(|_, item| item.deadline_ms > now);
        let floor = state.ack_floor.max(state.barrier);
        let ceiling = floor
            .saturating_add(self.max_ack_gap)
            .min(self.messages.len());
        if state.retention_cursor < floor || state.retention_cursor >= ceiling {
            state.retention_cursor = floor;
        }
        for _ in 0..ceiling.saturating_sub(floor) {
            if state.retention_cursor < floor || state.retention_cursor >= ceiling {
                state.retention_cursor = floor;
            }
            let position = state.retention_cursor;
            let message = &self.messages[position];
            state.retention_cursor += 1;
            if message.timestamp_ns > expired_before_ns
                || state.is_acknowledged(position, message.id)
                || state.in_flight.contains_key(&message.id)
                || state.loading.contains_key(&message.id)
                || state
                    .requeued_until
                    .get(&message.id)
                    .copied()
                    .unwrap_or(message.available_at_ms)
                    > now
            {
                continue;
            }
            let token = self.next_delivery_token;
            self.next_delivery_token = self.next_delivery_token.wrapping_add(1).max(1);
            let timeout_ms = duration_ms(timeout);
            state.loading.insert(
                message.id,
                LoadingDelivery {
                    token,
                    deadline_ms: now.saturating_add(timeout_ms.max(30_000)),
                },
            );
            return Ok(Some(ReservedDelivery {
                message_id: message.id,
                timestamp_ns: message.timestamp_ns,
                payload: message.payload.clone(),
                token,
                timeout_ms,
            }));
        }
        Ok(None)
    }

    pub(super) fn complete_delivery(
        &mut self,
        channel: &str,
        reservation: &ReservedDelivery,
        body: Arc<[u8]>,
    ) -> Result<Option<Delivery>, BrokerError> {
        let position = self.message_position(reservation.message_id);
        let state = self
            .channels
            .get_mut(channel)
            .ok_or(BrokerError::ChannelNotFound)?;
        let matches = state
            .loading
            .get(&reservation.message_id)
            .is_some_and(|loading| loading.token == reservation.token);
        if !matches {
            return Ok(None);
        }
        state.loading.remove(&reservation.message_id);
        state.delivery_blocked_until_ms = 0;
        let Some(position) = position else {
            return Ok(None);
        };
        if state.paused
            || !state.can_deliver(position)
            || state.is_acknowledged(position, reservation.message_id)
        {
            return Ok(None);
        }
        state.requeued_until.remove(&reservation.message_id);
        let attempts = state.attempts.entry(reservation.message_id).or_default();
        *attempts = attempts.saturating_add(1);
        state.in_flight.insert(
            reservation.message_id,
            InFlight {
                deadline_ms: now_ms().saturating_add(reservation.timeout_ms),
            },
        );
        Ok(Some(Delivery {
            id: reservation.message_id,
            timestamp_ns: reservation.timestamp_ns,
            attempts: *attempts,
            body,
        }))
    }

    pub(super) fn cancel_delivery(&mut self, channel: &str, reservation: &ReservedDelivery) {
        let Some(state) = self.channels.get_mut(channel) else {
            return;
        };
        if state
            .loading
            .get(&reservation.message_id)
            .is_some_and(|loading| loading.token == reservation.token)
        {
            state.loading.remove(&reservation.message_id);
            state.delivery_blocked_until_ms = 0;
        }
    }

    pub(super) fn finish(
        &mut self,
        channel: &str,
        id: u64,
        require_in_flight: bool,
    ) -> Result<(), BrokerError> {
        let position = self
            .message_position(id)
            .ok_or(BrokerError::MessageNotInFlight)?;
        let state = self
            .channels
            .get(channel)
            .ok_or(BrokerError::ChannelNotFound)?;
        if state.is_acknowledged(position, id) {
            return Ok(());
        }
        if require_in_flight && !state.in_flight.contains_key(&id) {
            return Err(BrokerError::MessageNotInFlight);
        }
        if !state.ephemeral {
            self.append_channel_command(
                RecordKind::Ack,
                &ChannelCommand {
                    channel: channel.to_owned(),
                    message_id: id,
                    available_at_ms: 0,
                    paused: false,
                },
            )?;
        }
        let state = self.channels.get_mut(channel).unwrap();
        state.in_flight.remove(&id);
        state.acknowledge(position, &self.messages);
        state.requeued_until.remove(&id);
        state.attempts.remove(&id);
        self.signal_delivery();
        Ok(())
    }

    pub(super) fn requeue(
        &mut self,
        channel: &str,
        id: u64,
        delay: Duration,
        require_in_flight: bool,
    ) -> Result<(), BrokerError> {
        let state = self
            .channels
            .get(channel)
            .ok_or(BrokerError::ChannelNotFound)?;
        if self.message_position(id).is_none()
            || (require_in_flight && !state.in_flight.contains_key(&id))
        {
            return Err(BrokerError::MessageNotInFlight);
        }
        let available_at_ms = now_ms().saturating_add(duration_ms(delay));
        self.requeue_at(channel, id, available_at_ms, require_in_flight)
    }

    pub(super) fn requeue_at(
        &mut self,
        channel: &str,
        id: u64,
        available_at_ms: i64,
        require_in_flight: bool,
    ) -> Result<(), BrokerError> {
        let state = self
            .channels
            .get(channel)
            .ok_or(BrokerError::ChannelNotFound)?;
        if self.message_position(id).is_none()
            || (require_in_flight && !state.in_flight.contains_key(&id))
        {
            return Err(BrokerError::MessageNotInFlight);
        }
        if !state.ephemeral {
            self.append_channel_command(
                RecordKind::Requeue,
                &ChannelCommand {
                    channel: channel.to_owned(),
                    message_id: id,
                    available_at_ms,
                    paused: false,
                },
            )?;
        }
        let state = self.channels.get_mut(channel).unwrap();
        state.in_flight.remove(&id);
        state.requeued_until.insert(id, available_at_ms);
        state.delivery_blocked_until_ms = 0;
        self.signal_delivery();
        Ok(())
    }

    pub(super) fn touch(
        &mut self,
        channel: &str,
        id: u64,
        timeout: Duration,
    ) -> Result<(), BrokerError> {
        let state = self
            .channels
            .get_mut(channel)
            .ok_or(BrokerError::ChannelNotFound)?;
        let in_flight = state
            .in_flight
            .get_mut(&id)
            .ok_or(BrokerError::MessageNotInFlight)?;
        in_flight.deadline_ms = now_ms().saturating_add(duration_ms(timeout));
        state.delivery_blocked_until_ms = 0;
        Ok(())
    }

    fn append_channel_command(
        &mut self,
        kind: RecordKind,
        command: &ChannelCommand,
    ) -> Result<(), BrokerError> {
        if !self.persist_wal {
            self.projection_index = self.projection_index.saturating_add(1);
            return Ok(());
        }
        let payload = serde_json::to_vec(command)
            .map_err(|error| BrokerError::InvalidRecord(error.to_string()))?;
        self.log.append(
            Record {
                kind,
                flags: 0,
                term: 1,
                index: 0,
                timestamp_ns: now_ns(),
                message_id: command.message_id,
                payload,
            },
            self.durable_appends,
        )?;
        self.dirty |= !self.durable_appends;
        Ok(())
    }
}
