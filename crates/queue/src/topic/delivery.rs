use super::*;
use crate::channel::{MessageAvailability, NextCandidate};
use crate::model::ReservedDelivery;
use crate::topic::index::{Lookup, PageRequest};

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
        let last = self.last_position();
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
            let (token, attempts) = channel.state.reserve(
                position,
                message.id,
                timeout.as_millis().min(i64::MAX as u128) as i64,
            );
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
        let command = self.finish_command(channel, id, require_in_flight)?;
        self.persist_channel(channel, command)
    }

    pub fn finish_buffered(
        &mut self,
        channel: &str,
        id: u64,
        require_in_flight: bool,
    ) -> Result<(), BrokerError> {
        let command = self.finish_command(channel, id, require_in_flight)?;
        self.persist_channel_buffered(channel, command)
    }

    fn finish_command(
        &self,
        channel: &str,
        id: u64,
        require_in_flight: bool,
    ) -> Result<ChannelCommand, BrokerError> {
        let position = if require_in_flight {
            self.channels
                .get(channel)
                .and_then(|channel| channel.state.in_flight_position(id))
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
    ) -> Result<(), BrokerError> {
        let command = self.requeue_command(channel, id, available_at_ms)?;
        self.persist_channel_buffered(channel, command)
    }

    fn requeue_command(
        &self,
        channel: &str,
        id: u64,
        available_at_ms: i64,
    ) -> Result<ChannelCommand, BrokerError> {
        let runtime = self
            .channels
            .get(channel)
            .ok_or(BrokerError::ChannelNotFound)?;
        let position = runtime
            .state
            .in_flight_position(id)
            .ok_or(BrokerError::MessageNotInFlight)?;
        let attempts = runtime.state.delivery_attempts(position);
        Ok(ChannelCommand::Requeue {
            position,
            message_id: id,
            available_at_ms,
            attempts,
        })
    }

    pub fn touch(&mut self, channel: &str, id: u64, timeout: Duration) -> Result<(), BrokerError> {
        let channel = self
            .channels
            .get_mut(channel)
            .ok_or(BrokerError::ChannelNotFound)?;
        let position = channel
            .state
            .in_flight_position(id)
            .ok_or(BrokerError::MessageNotInFlight)?;
        if channel
            .state
            .touch(position, timeout.as_millis().min(i64::MAX as u128) as i64)
        {
            Ok(())
        } else {
            Err(BrokerError::MessageNotInFlight)
        }
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

    pub fn release_all(&mut self) -> usize {
        self.channels
            .values_mut()
            .map(|channel| channel.state.release_all())
            .sum()
    }

    fn position_by_id(&self, id: u64) -> Result<Option<u64>, BrokerError> {
        self.messages.position_by_id(id)
    }
}
