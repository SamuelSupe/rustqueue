use super::*;
use crate::channel::{MessageAvailability, NextCandidate};
use crate::model::ReservedDelivery;
use crate::topic::index::{Lookup, PageRequest};
use std::time::Instant;

pub(crate) enum ReserveBatch {
    Ready(Vec<ReservedDelivery>),
    Load {
        reserved: Vec<ReservedDelivery>,
        request: PageRequest,
    },
}

impl Topic {
    pub fn reserve_batch(
        &mut self,
        channel_name: &str,
        max_messages: usize,
        max_bytes: usize,
        timeout: Duration,
    ) -> Result<ReserveBatch, BrokerError> {
        if self.manifest.deleted {
            return Err(BrokerError::TopicNotFound);
        }
        if self.manifest.paused {
            return Ok(ReserveBatch::Ready(Vec::new()));
        }
        let last = self.deliverable_position;
        let messages = &self.messages;
        let channel = self
            .channels
            .get_mut(channel_name)
            .ok_or(BrokerError::ChannelNotFound)?;
        let mut reserved = Vec::with_capacity(max_messages);
        let mut bytes = 0usize;
        for _ in 0..max_messages {
            let candidate = channel
                .state
                .next_candidate(now_ms(), last, |position| match messages.lookup(position) {
                    Lookup::Found(message) => MessageAvailability::Ready(message.available_at_ms),
                    Lookup::Load(_) => MessageAvailability::Missing,
                    Lookup::Absent => MessageAvailability::Absent,
                });
            let position = match candidate {
                NextCandidate::Ready(position) => position,
                NextCandidate::Load(position) => match messages.lookup(position) {
                    Lookup::Load(request) => {
                        return Ok(ReserveBatch::Load { reserved, request });
                    }
                    Lookup::Found(_) | Lookup::Absent => continue,
                },
                NextCandidate::None => break,
            };
            let Lookup::Found(message) = messages.lookup(position) else {
                continue;
            };
            let next_bytes = message.payload.len as usize;
            if !reserved.is_empty() && bytes.saturating_add(next_bytes) > max_bytes {
                channel.state.defer_candidate(position);
                break;
            }
            let (token, attempts) = channel.state.reserve(position, message.id, timeout);
            bytes = bytes.saturating_add(next_bytes);
            reserved.push(ReservedDelivery {
                position,
                id: message.id,
                timestamp_ns: message.timestamp_ns,
                attempts,
                token,
                payload: message.payload.clone(),
            });
        }
        Ok(ReserveBatch::Ready(reserved))
    }

    pub fn cancel(&mut self, channel: &str, reservations: &[ReservedDelivery]) {
        if let Some(channel) = self.channels.get_mut(channel) {
            for reservation in reservations {
                channel
                    .state
                    .cancel(reservation.position, reservation.token);
            }
        }
    }

    pub fn finish(
        &mut self,
        channel: &str,
        id: u64,
        require_in_flight: bool,
    ) -> Result<(), BrokerError> {
        let command = self.finish_command(channel, id, require_in_flight, None)?;
        self.persist_channel(channel, command)
    }

    pub fn finish_buffered(
        &mut self,
        channel: &str,
        id: u64,
        require_in_flight: bool,
        token: Option<u64>,
    ) -> Result<(), BrokerError> {
        let command = self.finish_command(channel, id, require_in_flight, token)?;
        self.persist_channel_buffered(channel, command)
    }

    fn finish_command(
        &self,
        channel: &str,
        id: u64,
        require_in_flight: bool,
        token: Option<u64>,
    ) -> Result<ChannelCommand, BrokerError> {
        let position = if require_in_flight {
            let state = &self
                .channels
                .get(channel)
                .ok_or(BrokerError::ChannelNotFound)?
                .state;
            match token {
                Some(token) => state.in_flight_position_with_token(id, token),
                None => state.in_flight_position(id),
            }
            .ok_or(BrokerError::MessageNotInFlight)?
        } else {
            self.position_by_id(id)?
                .ok_or(BrokerError::MessageNotFound)?
        };
        Ok(ChannelCommand::Finish {
            position,
            message_id: id,
        })
    }

    pub fn requeue_buffered(
        &mut self,
        channel: &str,
        id: u64,
        available_at_ms: i64,
        token: Option<u64>,
    ) -> Result<(), BrokerError> {
        let command = self.requeue_command(channel, id, available_at_ms, token)?;
        self.persist_channel_buffered(channel, command)
    }

    fn requeue_command(
        &self,
        channel: &str,
        id: u64,
        available_at_ms: i64,
        token: Option<u64>,
    ) -> Result<ChannelCommand, BrokerError> {
        let runtime = self
            .channels
            .get(channel)
            .ok_or(BrokerError::ChannelNotFound)?;
        let position = match token {
            Some(token) => runtime.state.in_flight_position_with_token(id, token),
            None => runtime.state.in_flight_position(id),
        }
        .ok_or(BrokerError::MessageNotInFlight)?;
        let attempts = runtime.state.delivery_attempts(position);
        Ok(ChannelCommand::Requeue {
            position,
            message_id: id,
            available_at_ms,
            attempts,
            cumulative_count: runtime
                .durable_counters
                .then(|| runtime.state.next_requeue_count()),
        })
    }

    pub fn touch(&mut self, channel: &str, id: u64, timeout: Duration) -> Result<(), BrokerError> {
        self.touch_with_token(channel, id, None, timeout)
    }

    pub fn touch_with_token(
        &mut self,
        channel: &str,
        id: u64,
        token: Option<u64>,
        timeout: Duration,
    ) -> Result<(), BrokerError> {
        let channel = self
            .channels
            .get_mut(channel)
            .ok_or(BrokerError::ChannelNotFound)?;
        let position = match token {
            Some(token) => channel.state.in_flight_position_with_token(id, token),
            None => channel.state.in_flight_position(id),
        }
        .ok_or(BrokerError::MessageNotInFlight)?;
        if channel.state.touch(position, timeout) {
            Ok(())
        } else {
            Err(BrokerError::MessageNotInFlight)
        }
    }

    pub fn touch_deliveries_with_tokens(
        &mut self,
        channel: &str,
        deliveries: &[(u64, u64)],
        timeout: Duration,
    ) -> Result<(), BrokerError> {
        let channel = self
            .channels
            .get_mut(channel)
            .ok_or(BrokerError::ChannelNotFound)?;
        let positions: Vec<_> = deliveries
            .iter()
            .map(|(id, token)| {
                channel
                    .state
                    .in_flight_position_with_token(*id, *token)
                    .ok_or(BrokerError::MessageNotInFlight)
            })
            .collect::<Result<_, _>>()?;
        let deadline = Instant::now() + timeout;
        for position in positions {
            debug_assert!(channel.state.touch_until(position, deadline));
        }
        Ok(())
    }

    pub fn release(&mut self, channel: &str, ids: &[u64]) {
        let Some(channel) = self.channels.get_mut(channel) else {
            return;
        };
        for id in ids {
            if let Some(position) = channel.state.in_flight_position(*id) {
                channel.state.release(position);
            }
        }
    }

    pub fn release_with_tokens(&mut self, channel: &str, deliveries: &[(u64, u64)]) {
        let Some(channel) = self.channels.get_mut(channel) else {
            return;
        };
        for (id, token) in deliveries {
            if let Some(position) = channel.state.in_flight_position_with_token(*id, *token) {
                channel.state.release(position);
            }
        }
    }

    pub fn release_all(&mut self) -> usize {
        self.channels
            .values_mut()
            .map(|channel| channel.state.release_all())
            .sum()
    }

    pub fn message_is_unacknowledged(
        &self,
        channel: &str,
        id: u64,
    ) -> Result<Option<bool>, BrokerError> {
        let Some(position) = self.position_by_id(id)? else {
            return Ok(None);
        };
        Ok(self
            .channels
            .get(channel)
            .map(|runtime| runtime.state.is_unacknowledged(position)))
    }

    fn position_by_id(&self, id: u64) -> Result<Option<u64>, BrokerError> {
        self.messages.position_by_id(id)
    }
}
