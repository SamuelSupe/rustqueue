use crate::channel_store::ChannelStore;
use crate::model::ChannelStats;
use crate::BrokerError;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) enum ChannelCommand {
    Finish {
        position: u64,
        message_id: u64,
    },
    Requeue {
        position: u64,
        message_id: u64,
        available_at_ms: i64,
        attempts: u16,
    },
    Pause {
        paused: bool,
    },
    Empty {
        through_position: u64,
    },
    Evict {
        through_position: u64,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct ChannelCheckpoint {
    pub format: u8,
    pub name: String,
    pub barrier_position: u64,
    pub ack_floor_position: u64,
    pub acknowledged: BTreeSet<u64>,
    pub requeued_until: BTreeMap<u64, i64>,
    #[serde(default)]
    pub attempts: BTreeMap<u64, u16>,
    pub paused: bool,
    pub ephemeral: bool,
}

struct InFlight {
    deadline: Instant,
    token: u64,
}

pub(crate) struct ChannelRuntime {
    pub state: ChannelState,
    pub store: Option<ChannelStore>,
}

pub(crate) struct ChannelState {
    pub name: String,
    pub barrier_position: u64,
    pub ack_floor_position: u64,
    pub acknowledged: BTreeSet<u64>,
    pub requeued_until: BTreeMap<u64, i64>,
    pub paused: bool,
    pub ephemeral: bool,
    next_position: u64,
    in_flight: HashMap<u64, InFlight>,
    in_flight_ids: HashMap<u64, u64>,
    redelivery: BTreeSet<u64>,
    attempts: HashMap<u64, u16>,
    next_token: u64,
    max_ack_gap: usize,
}

pub(crate) enum MessageAvailability {
    Ready(i64),
    Missing,
    Absent,
}

pub(crate) enum NextCandidate {
    Ready(u64),
    Load(u64),
    None,
}

impl ChannelState {
    pub fn new(name: String, barrier_position: u64, ephemeral: bool, max_ack_gap: usize) -> Self {
        Self {
            name,
            barrier_position,
            ack_floor_position: barrier_position,
            acknowledged: BTreeSet::new(),
            requeued_until: BTreeMap::new(),
            paused: false,
            ephemeral,
            next_position: barrier_position.saturating_add(1),
            in_flight: HashMap::new(),
            in_flight_ids: HashMap::new(),
            redelivery: BTreeSet::new(),
            attempts: HashMap::new(),
            next_token: 1,
            max_ack_gap: max_ack_gap.max(1),
        }
    }

    pub fn from_checkpoint(
        checkpoint: ChannelCheckpoint,
        max_ack_gap: usize,
    ) -> Result<Self, BrokerError> {
        if checkpoint.format != 7 || checkpoint.ack_floor_position < checkpoint.barrier_position {
            return Err(BrokerError::InvalidRecord(
                "invalid channel checkpoint".into(),
            ));
        }
        let next_position = checkpoint.ack_floor_position.saturating_add(1);
        Ok(Self {
            name: checkpoint.name,
            barrier_position: checkpoint.barrier_position,
            ack_floor_position: checkpoint.ack_floor_position,
            acknowledged: checkpoint.acknowledged,
            requeued_until: checkpoint.requeued_until,
            paused: checkpoint.paused,
            ephemeral: checkpoint.ephemeral,
            next_position,
            in_flight: HashMap::new(),
            in_flight_ids: HashMap::new(),
            redelivery: BTreeSet::new(),
            attempts: checkpoint.attempts.into_iter().collect(),
            next_token: 1,
            max_ack_gap: max_ack_gap.max(1),
        })
    }

    pub fn checkpoint(&self) -> ChannelCheckpoint {
        ChannelCheckpoint {
            format: 7,
            name: self.name.clone(),
            barrier_position: self.barrier_position,
            ack_floor_position: self.ack_floor_position,
            acknowledged: self.acknowledged.clone(),
            requeued_until: self.requeued_until.clone(),
            attempts: self
                .attempts
                .iter()
                .map(|(position, attempts)| (*position, *attempts))
                .collect(),
            paused: self.paused,
            ephemeral: self.ephemeral,
        }
    }

    pub fn apply(&mut self, command: &ChannelCommand) {
        match *command {
            ChannelCommand::Finish { position, .. } => self.acknowledge(position),
            ChannelCommand::Requeue {
                position,
                available_at_ms,
                attempts,
                ..
            } => {
                self.remove_in_flight(position);
                self.redelivery.insert(position);
                self.requeued_until.insert(position, available_at_ms);
                self.attempts.insert(position, attempts);
            }
            ChannelCommand::Pause { paused } => self.paused = paused,
            ChannelCommand::Empty { through_position }
            | ChannelCommand::Evict { through_position } => {
                self.ack_floor_position = self.ack_floor_position.max(through_position);
                self.acknowledged
                    .retain(|position| *position > through_position);
                self.requeued_until
                    .retain(|position, _| *position > through_position);
                self.attempts
                    .retain(|position, _| *position > through_position);
                let removed: Vec<_> = self
                    .in_flight
                    .keys()
                    .copied()
                    .filter(|position| *position <= through_position)
                    .collect();
                for position in removed {
                    self.remove_in_flight(position);
                }
                self.redelivery
                    .retain(|position| *position > through_position);
                self.next_position = self.next_position.max(through_position.saturating_add(1));
            }
        }
    }

    pub fn next_candidate(
        &mut self,
        now_ms: i64,
        last_position: u64,
        available_at: impl Fn(u64) -> MessageAvailability,
    ) -> NextCandidate {
        if self.paused {
            return NextCandidate::None;
        }
        self.expire_in_flight();
        let mut absent = Vec::new();
        let redelivery: Vec<_> = self.redelivery.iter().copied().collect();
        for position in redelivery {
            if self
                .requeued_until
                .get(&position)
                .copied()
                .unwrap_or_default()
                > now_ms
            {
                continue;
            }
            match available_at(position) {
                MessageAvailability::Ready(available) if available <= now_ms => {
                    self.redelivery.remove(&position);
                    self.requeued_until.remove(&position);
                    return NextCandidate::Ready(position);
                }
                MessageAvailability::Missing => return NextCandidate::Load(position),
                MessageAvailability::Absent => absent.push(position),
                MessageAvailability::Ready(_) => {}
            }
        }
        for position in absent {
            self.redelivery.remove(&position);
            self.requeued_until.remove(&position);
        }
        while self.next_position <= last_position {
            if self.next_position
                > self
                    .ack_floor_position
                    .saturating_add(self.max_ack_gap as u64)
            {
                return NextCandidate::None;
            }
            let position = self.next_position;
            if position <= self.ack_floor_position
                || self.acknowledged.contains(&position)
                || self.in_flight.contains_key(&position)
            {
                self.next_position = self.next_position.saturating_add(1);
                continue;
            }
            let available = match available_at(position) {
                MessageAvailability::Ready(available) => available,
                MessageAvailability::Missing => return NextCandidate::Load(position),
                MessageAvailability::Absent => {
                    self.next_position = self.next_position.saturating_add(1);
                    continue;
                }
            };
            self.next_position = self.next_position.saturating_add(1);
            if available > now_ms {
                self.redelivery.insert(position);
                self.requeued_until.insert(position, available);
                continue;
            }
            return NextCandidate::Ready(position);
        }
        NextCandidate::None
    }

    pub fn reserve(&mut self, position: u64, id: u64, timeout: Duration) -> (u64, u16) {
        let token = self.next_token;
        self.next_token = self.next_token.wrapping_add(1).max(1);
        let attempts = self.attempts.entry(position).or_insert(0);
        *attempts = attempts.saturating_add(1);
        self.in_flight.insert(
            position,
            InFlight {
                deadline: Instant::now() + timeout,
                token,
            },
        );
        self.in_flight_ids.insert(id, position);
        (token, *attempts)
    }

    pub fn cancel(&mut self, position: u64, token: u64) {
        if self
            .in_flight
            .get(&position)
            .is_some_and(|flight| flight.token == token)
        {
            self.remove_in_flight(position);
            self.redelivery.insert(position);
            if let Some(attempts) = self.attempts.get_mut(&position) {
                *attempts = attempts.saturating_sub(1);
                if *attempts == 0 {
                    self.attempts.remove(&position);
                }
            }
        }
    }

    pub fn defer_candidate(&mut self, position: u64) {
        self.redelivery.insert(position);
    }

    pub fn in_flight_position(&self, id: u64) -> Option<u64> {
        self.in_flight_ids.get(&id).copied()
    }

    pub fn delivery_attempts(&self, position: u64) -> u16 {
        self.attempts.get(&position).copied().unwrap_or_default()
    }

    pub fn touch(&mut self, position: u64, timeout: Duration) -> bool {
        let Some(flight) = self.in_flight.get_mut(&position) else {
            return false;
        };
        flight.deadline = Instant::now() + timeout;
        true
    }

    pub fn release(&mut self, position: u64) {
        if self.remove_in_flight(position) {
            self.redelivery.insert(position);
        }
    }

    pub fn release_all(&mut self) -> usize {
        let positions: Vec<_> = self.in_flight.keys().copied().collect();
        let count = positions.len();
        for position in positions {
            self.remove_in_flight(position);
            self.redelivery.insert(position);
        }
        count
    }

    pub fn expire_in_flight(&mut self) -> usize {
        let now = Instant::now();
        let expired: Vec<_> = self
            .in_flight
            .iter()
            .filter_map(|(position, flight)| (flight.deadline <= now).then_some(*position))
            .collect();
        let count = expired.len();
        for position in expired {
            self.remove_in_flight(position);
            self.redelivery.insert(position);
        }
        count
    }

    pub fn first_in_flight_position(&self) -> Option<u64> {
        self.in_flight.keys().copied().min()
    }

    pub fn stats(&self, last_position: u64) -> ChannelStats {
        let (depth, in_flight_count, deferred_count, ack_gap) = self.metric_counts(last_position);
        ChannelStats {
            name: self.name.clone(),
            depth,
            in_flight_count,
            deferred_count,
            paused: self.paused,
            ephemeral: self.ephemeral,
            ack_cursor: self.ack_floor_position,
            ack_gap,
        }
    }

    pub fn metric_counts(&self, last_position: u64) -> (u64, u64, u64, u64) {
        let total = last_position.saturating_sub(self.ack_floor_position);
        (
            total.saturating_sub(self.acknowledged.len() as u64),
            self.in_flight.len() as u64,
            self.requeued_until.len() as u64,
            self.acknowledged.len() as u64,
        )
    }

    fn acknowledge(&mut self, position: u64) {
        self.remove_in_flight(position);
        self.redelivery.remove(&position);
        self.requeued_until.remove(&position);
        self.attempts.remove(&position);
        if position <= self.ack_floor_position {
            return;
        }
        self.acknowledged.insert(position);
        while self
            .acknowledged
            .remove(&self.ack_floor_position.saturating_add(1))
        {
            self.ack_floor_position = self.ack_floor_position.saturating_add(1);
        }
    }

    fn remove_in_flight(&mut self, position: u64) -> bool {
        let removed = self.in_flight.remove(&position).is_some();
        if removed {
            self.in_flight_ids
                .retain(|_, candidate| *candidate != position);
        }
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancelling_an_unhanded_delivery_restores_attempt_count() {
        let mut channel = ChannelState::new("workers".into(), 0, false, 16);
        let (token, attempts) = channel.reserve(1, 10, Duration::from_secs(30));
        assert_eq!(attempts, 1);
        channel.cancel(1, token);
        assert_eq!(channel.delivery_attempts(1), 0);

        let (_, attempts) = channel.reserve(1, 10, Duration::from_secs(30));
        assert_eq!(attempts, 1);
    }

    #[test]
    fn in_flight_expiry_uses_a_monotonic_deadline() {
        let mut channel = ChannelState::new("workers".into(), 0, false, 16);
        channel.reserve(1, 10, Duration::ZERO);
        assert_eq!(channel.expire_in_flight(), 1);
        assert_eq!(channel.in_flight_position(10), None);
    }
}
