use crate::channel_store::ChannelStore;
use crate::model::ChannelStats;
use crate::BrokerError;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
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
        #[serde(default)]
        cumulative_count: Option<u64>,
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
    Timeout {
        cumulative_count: u64,
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
    #[serde(default)]
    pub message_count_origin_position: Option<u64>,
    #[serde(default)]
    pub requeue_count: u64,
    #[serde(default)]
    pub timeout_count: u64,
    pub paused: bool,
    pub ephemeral: bool,
}

struct InFlight {
    id: u64,
    deadline: Instant,
    token: u64,
}

pub(crate) struct ChannelRuntime {
    pub state: ChannelState,
    pub store: Option<ChannelStore>,
    pub durable_counters: bool,
}

pub(crate) struct ChannelState {
    pub name: String,
    pub barrier_position: u64,
    pub ack_floor_position: u64,
    pub acknowledged: BTreeSet<u64>,
    pub requeued_until: BTreeMap<u64, i64>,
    pub paused: bool,
    pub ephemeral: bool,
    message_count_origin_position: u64,
    next_position: u64,
    in_flight: HashMap<u64, InFlight>,
    in_flight_ids: HashMap<u64, u64>,
    in_flight_deadlines: BTreeSet<(Instant, u64, u64)>,
    redelivery: BTreeSet<u64>,
    attempts: HashMap<u64, u16>,
    next_token: u64,
    max_ack_gap: usize,
    requeue_count: u64,
    timeout_count: u64,
    // Rebuilt from Topic segment ranges on open. Lost relaxed positions are
    // never reused, so this shared index needs no additional v7 persistence.
    absent_ranges: Arc<[(u64, u64)]>,
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
            message_count_origin_position: barrier_position,
            next_position: barrier_position.saturating_add(1),
            in_flight: HashMap::new(),
            in_flight_ids: HashMap::new(),
            in_flight_deadlines: BTreeSet::new(),
            redelivery: BTreeSet::new(),
            attempts: HashMap::new(),
            next_token: 1,
            max_ack_gap: max_ack_gap.max(1),
            requeue_count: 0,
            timeout_count: 0,
            absent_ranges: Arc::from(Vec::new()),
        }
    }

    pub fn from_checkpoint(
        checkpoint: ChannelCheckpoint,
        max_ack_gap: usize,
    ) -> Result<Self, BrokerError> {
        let message_count_origin_position = checkpoint
            .message_count_origin_position
            .unwrap_or(checkpoint.barrier_position);
        if checkpoint.format != 7
            || checkpoint.ack_floor_position < checkpoint.barrier_position
            || message_count_origin_position > checkpoint.barrier_position
        {
            return Err(BrokerError::InvalidRecord(
                "invalid channel checkpoint".into(),
            ));
        }
        let next_position = checkpoint.ack_floor_position.saturating_add(1);
        let redelivery = checkpoint.requeued_until.keys().copied().collect();
        Ok(Self {
            name: checkpoint.name,
            barrier_position: checkpoint.barrier_position,
            ack_floor_position: checkpoint.ack_floor_position,
            acknowledged: checkpoint.acknowledged,
            requeued_until: checkpoint.requeued_until,
            paused: checkpoint.paused,
            ephemeral: checkpoint.ephemeral,
            message_count_origin_position,
            next_position,
            in_flight: HashMap::new(),
            in_flight_ids: HashMap::new(),
            in_flight_deadlines: BTreeSet::new(),
            redelivery,
            attempts: checkpoint.attempts.into_iter().collect(),
            next_token: 1,
            max_ack_gap: max_ack_gap.max(1),
            requeue_count: checkpoint.requeue_count,
            timeout_count: checkpoint.timeout_count,
            absent_ranges: Arc::from(Vec::new()),
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
            message_count_origin_position: Some(self.message_count_origin_position),
            requeue_count: self.requeue_count,
            timeout_count: self.timeout_count,
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
                cumulative_count,
                ..
            } => {
                let already_applied = self.requeued_until.get(&position) == Some(&available_at_ms)
                    && self.attempts.get(&position) == Some(&attempts);
                self.remove_in_flight(position);
                self.redelivery.insert(position);
                self.requeued_until.insert(position, available_at_ms);
                self.attempts.insert(position, attempts);
                if let Some(count) = cumulative_count {
                    self.requeue_count = self.requeue_count.max(count);
                } else if !already_applied {
                    self.requeue_count = self.requeue_count.saturating_add(1);
                }
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
            ChannelCommand::Timeout { cumulative_count } => {
                self.timeout_count = self.timeout_count.max(cumulative_count);
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
        let mut absent = Vec::new();
        let redelivery: Vec<_> = self.redelivery.iter().copied().collect();
        for position in redelivery {
            if self.is_absent(position) {
                self.redelivery.remove(&position);
                self.requeued_until.remove(&position);
                self.attempts.remove(&position);
                self.advance_ack_floor();
                continue;
            }
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
                MessageAvailability::Absent => {
                    absent.push(position);
                    self.acknowledge(position);
                }
                MessageAvailability::Ready(_) => {}
            }
        }
        for position in absent {
            self.redelivery.remove(&position);
            self.requeued_until.remove(&position);
        }
        while self.next_position <= last_position {
            if let Some(end) = self.absent_range_end(self.next_position) {
                self.next_position = end.saturating_add(1);
                self.advance_ack_floor();
                continue;
            }
            if self.present_distance(self.next_position) > self.max_ack_gap as u64 {
                return NextCandidate::None;
            }
            let position = self.next_position;
            if position <= self.ack_floor_position
                || self.acknowledged.contains(&position)
                || self.in_flight.contains_key(&position)
                || self.redelivery.contains(&position)
            {
                self.next_position = self.next_position.saturating_add(1);
                continue;
            }
            let available = match available_at(position) {
                MessageAvailability::Ready(available) => available,
                MessageAvailability::Missing => return NextCandidate::Load(position),
                MessageAvailability::Absent => {
                    self.next_position = self.next_position.saturating_add(1);
                    self.acknowledge(position);
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

    pub fn set_absent_ranges(&mut self, ranges: Arc<[(u64, u64)]>) {
        debug_assert!(ranges
            .windows(2)
            .all(|pair| pair[0].0 <= pair[0].1 && pair[0].1 < pair[1].0));
        debug_assert!(ranges.last().is_none_or(|range| range.0 <= range.1));
        debug_assert!(self.in_flight.is_empty());
        self.acknowledged
            .retain(|position| !position_in_ranges(&ranges, *position));
        self.redelivery
            .retain(|position| !position_in_ranges(&ranges, *position));
        self.requeued_until
            .retain(|position, _| !position_in_ranges(&ranges, *position));
        self.attempts
            .retain(|position, _| !position_in_ranges(&ranges, *position));
        self.absent_ranges = ranges;
        self.advance_ack_floor();
    }

    pub fn recovered_position_high_watermark(&self) -> u64 {
        self.ack_floor_position
            .max(self.acknowledged.last().copied().unwrap_or(0))
            .max(
                self.requeued_until
                    .last_key_value()
                    .map_or(0, |(position, _)| *position),
            )
            .max(self.attempts.keys().copied().max().unwrap_or(0))
    }

    pub fn reserve(&mut self, position: u64, id: u64, timeout: Duration) -> (u64, u16) {
        let token = self.next_token;
        self.next_token = self.next_token.wrapping_add(1).max(1);
        let attempts = self.attempts.entry(position).or_insert(0);
        *attempts = attempts.saturating_add(1);
        let deadline = Instant::now() + timeout;
        self.in_flight.insert(
            position,
            InFlight {
                id,
                deadline,
                token,
            },
        );
        self.in_flight_deadlines.insert((deadline, position, token));
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

    pub fn in_flight_position_with_token(&self, id: u64, token: u64) -> Option<u64> {
        let position = self.in_flight_position(id)?;
        self.in_flight
            .get(&position)
            .is_some_and(|flight| flight.token == token)
            .then_some(position)
    }

    pub fn is_unacknowledged(&self, position: u64) -> bool {
        position > self.ack_floor_position && !self.acknowledged.contains(&position)
    }

    pub fn delivery_attempts(&self, position: u64) -> u16 {
        self.attempts.get(&position).copied().unwrap_or_default()
    }

    pub fn next_requeue_count(&self) -> u64 {
        self.requeue_count.saturating_add(1)
    }

    pub fn touch(&mut self, position: u64, timeout: Duration) -> bool {
        self.touch_until(position, Instant::now() + timeout)
    }

    pub fn touch_until(&mut self, position: u64, deadline: Instant) -> bool {
        let Some(flight) = self.in_flight.get(&position) else {
            return false;
        };
        self.in_flight_deadlines
            .remove(&(flight.deadline, position, flight.token));
        let token = flight.token;
        self.in_flight
            .get_mut(&position)
            .expect("in-flight delivery remains present")
            .deadline = deadline;
        self.in_flight_deadlines.insert((deadline, position, token));
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

    fn expired_positions(&self, now: Instant) -> Vec<u64> {
        self.in_flight_deadlines
            .range(..=(now, u64::MAX, u64::MAX))
            .map(|(_, position, _)| *position)
            .collect()
    }

    fn has_expired_in_flight(&self, now: Instant) -> bool {
        self.in_flight_deadlines
            .first()
            .is_some_and(|(deadline, _, _)| *deadline <= now)
    }

    fn expire_positions(&mut self, positions: &[u64]) {
        for position in positions {
            self.remove_in_flight(*position);
            self.redelivery.insert(*position);
        }
    }

    pub fn first_in_flight_position(&self) -> Option<u64> {
        self.in_flight.keys().copied().min()
    }

    pub fn stats(
        &self,
        last_position: u64,
        scheduled: &BTreeSet<u64>,
        now_ms: i64,
    ) -> ChannelStats {
        let (depth, in_flight_count, deferred_count, ack_gap) =
            self.metric_counts(last_position, scheduled, now_ms);
        ChannelStats {
            name: self.name.clone(),
            depth,
            message_count: last_position.saturating_sub(self.message_count_origin_position),
            in_flight_count,
            deferred_count,
            requeue_count: self.requeue_count,
            timeout_count: self.timeout_count,
            paused: self.paused,
            ephemeral: self.ephemeral,
            ack_cursor: self.ack_floor_position,
            ack_gap,
        }
    }

    pub fn metric_counts(
        &self,
        last_position: u64,
        scheduled: &BTreeSet<u64>,
        now_ms: i64,
    ) -> (u64, u64, u64, u64) {
        let total = last_position
            .saturating_sub(self.ack_floor_position)
            .saturating_sub(
                self.absent_count(self.ack_floor_position.saturating_add(1), last_position),
            );
        let scheduled_count = scheduled
            .iter()
            .filter(|position| self.is_outstanding(**position, last_position))
            .count() as u64;
        let requeued_count = self
            .requeued_until
            .iter()
            .filter(|(position, until)| {
                **until > now_ms
                    && !scheduled.contains(position)
                    && self.is_outstanding(**position, last_position)
            })
            .count() as u64;
        (
            total.saturating_sub(self.acknowledged.len() as u64),
            self.in_flight.len() as u64,
            scheduled_count.saturating_add(requeued_count),
            self.acknowledged.len() as u64,
        )
    }

    fn is_outstanding(&self, position: u64, last_position: u64) -> bool {
        position > self.ack_floor_position
            && position <= last_position
            && !self.is_absent(position)
            && !self.acknowledged.contains(&position)
            && !self.in_flight.contains_key(&position)
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
        self.advance_ack_floor();
    }

    fn advance_ack_floor(&mut self) {
        loop {
            let next = self.ack_floor_position.saturating_add(1);
            if next == self.ack_floor_position {
                break;
            }
            if let Some(end) = self.absent_range_end(next) {
                self.ack_floor_position = end;
                continue;
            }
            if self.acknowledged.remove(&next) {
                self.ack_floor_position = next;
                continue;
            }
            break;
        }
        self.next_position = self
            .next_position
            .max(self.ack_floor_position.saturating_add(1));
    }

    fn is_absent(&self, position: u64) -> bool {
        self.absent_range_end(position).is_some()
    }

    fn absent_range_end(&self, position: u64) -> Option<u64> {
        let index = self
            .absent_ranges
            .partition_point(|(_, end)| *end < position);
        self.absent_ranges
            .get(index)
            .filter(|(start, _)| position >= *start)
            .map(|(_, end)| *end)
    }

    fn absent_count(&self, first: u64, last: u64) -> u64 {
        if first > last {
            return 0;
        }
        self.absent_ranges
            .iter()
            .map(|(range_first, range_last)| {
                let overlap_first = first.max(*range_first);
                let overlap_last = last.min(*range_last);
                if overlap_first <= overlap_last {
                    overlap_last.saturating_sub(overlap_first).saturating_add(1)
                } else {
                    0
                }
            })
            .fold(0u64, u64::saturating_add)
    }

    fn present_distance(&self, position: u64) -> u64 {
        position
            .saturating_sub(self.ack_floor_position)
            .saturating_sub(self.absent_count(self.ack_floor_position.saturating_add(1), position))
    }

    fn remove_in_flight(&mut self, position: u64) -> bool {
        let Some(delivery) = self.in_flight.remove(&position) else {
            return false;
        };
        self.in_flight_deadlines
            .remove(&(delivery.deadline, position, delivery.token));
        self.in_flight_ids.remove(&delivery.id);
        true
    }
}

fn position_in_ranges(ranges: &[(u64, u64)], position: u64) -> bool {
    let index = ranges.partition_point(|(_, end)| *end < position);
    ranges
        .get(index)
        .is_some_and(|(start, _)| position >= *start)
}

impl ChannelRuntime {
    pub fn has_expired_in_flight(&self) -> bool {
        self.state.has_expired_in_flight(Instant::now())
    }

    pub fn expire_in_flight(&mut self) -> Result<usize, BrokerError> {
        let positions = self.state.expired_positions(Instant::now());
        if positions.is_empty() {
            return Ok(0);
        }
        let cumulative_count = self
            .state
            .timeout_count
            .saturating_add(positions.len() as u64);
        if self.durable_counters {
            let command = ChannelCommand::Timeout { cumulative_count };
            if let Some(store) = self.store.as_mut() {
                store.append(&command)?;
            }
            self.state.expire_positions(&positions);
            self.state.apply(&command);
        } else {
            self.state.expire_positions(&positions);
            self.state.timeout_count = cumulative_count;
        }
        if let Some(store) = self.store.as_mut() {
            store.checkpoint_if_needed(&self.state)?;
        }
        Ok(positions.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovered_position_gaps_do_not_consume_the_ack_window() {
        let mut channel = ChannelState::new("workers".into(), 0, false, 2);
        channel.set_absent_ranges(Arc::from(vec![(2, 10)]));
        let stats = channel.stats(11, &BTreeSet::new(), i64::MAX);
        assert_eq!(stats.depth, 2);
        assert_eq!(stats.message_count, 11);

        assert!(matches!(
            channel.next_candidate(0, 11, |position| match position {
                1 | 11 => MessageAvailability::Ready(0),
                _ => panic!("known gaps must not be looked up"),
            }),
            NextCandidate::Ready(1)
        ));
        channel.reserve(1, 100, Duration::from_secs(30));
        assert!(matches!(
            channel.next_candidate(0, 11, |position| match position {
                1 | 11 => MessageAvailability::Ready(0),
                _ => panic!("known gaps must not be looked up"),
            }),
            NextCandidate::Ready(11)
        ));
        channel.reserve(11, 110, Duration::from_secs(30));
        channel.apply(&ChannelCommand::Finish {
            position: 11,
            message_id: 110,
        });
        channel.apply(&ChannelCommand::Finish {
            position: 1,
            message_id: 100,
        });

        let stats = channel.stats(11, &BTreeSet::new(), i64::MAX);
        assert_eq!(stats.ack_cursor, 11);
        assert_eq!(stats.ack_gap, 0);
        assert_eq!(stats.depth, 0);
    }

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
    fn removing_one_delivery_preserves_other_in_flight_id_lookups() {
        let mut channel = ChannelState::new("workers".into(), 0, false, 16);
        channel.reserve(1, 10, Duration::from_secs(30));
        channel.reserve(2, 20, Duration::from_secs(30));

        channel.apply(&ChannelCommand::Finish {
            position: 1,
            message_id: 10,
        });

        assert_eq!(channel.in_flight_position(10), None);
        assert_eq!(channel.in_flight_position(20), Some(2));
    }

    #[test]
    fn in_flight_expiry_uses_a_monotonic_deadline() {
        let mut channel = ChannelState::new("workers".into(), 0, false, 16);
        channel.reserve(1, 10, Duration::ZERO);
        let mut runtime = ChannelRuntime {
            state: channel,
            store: None,
            durable_counters: true,
        };
        assert_eq!(runtime.expire_in_flight().unwrap(), 1);
        let channel = runtime.state;
        assert_eq!(channel.in_flight_position(10), None);
        assert_eq!(
            channel.stats(1, &BTreeSet::new(), i64::MAX).timeout_count,
            1
        );
    }

    #[test]
    fn in_flight_deadline_index_tracks_touch_and_finish() {
        let mut channel = ChannelState::new("workers".into(), 0, false, 16);
        channel.reserve(1, 10, Duration::from_secs(60));
        channel.reserve(2, 20, Duration::from_secs(60));

        let now = Instant::now();
        assert!(channel.touch_until(1, now - Duration::from_secs(1)));
        assert_eq!(channel.expired_positions(now), vec![1]);

        channel.apply(&ChannelCommand::Finish {
            position: 1,
            message_id: 10,
        });
        assert!(channel.expired_positions(now).is_empty());
    }

    #[test]
    fn cumulative_counters_and_message_count_survive_checkpoint_and_empty() {
        let mut channel = ChannelState::new("workers".into(), 0, false, 16);
        channel.reserve(1, 10, Duration::ZERO);
        let mut runtime = ChannelRuntime {
            state: channel,
            store: None,
            durable_counters: true,
        };
        runtime.state.apply(&ChannelCommand::Requeue {
            position: 1,
            message_id: 10,
            available_at_ms: 0,
            attempts: 1,
            cumulative_count: Some(1),
        });
        runtime.state.reserve(2, 11, Duration::ZERO);
        runtime.expire_in_flight().unwrap();
        runtime.state.apply(&ChannelCommand::Empty {
            through_position: 2,
        });
        let current = runtime.state.stats(2, &BTreeSet::new(), i64::MAX);
        assert_eq!(current.message_count, 2);
        assert_eq!(current.requeue_count, 1);
        assert_eq!(current.timeout_count, 1);

        let recovered = ChannelState::from_checkpoint(runtime.state.checkpoint(), 16).unwrap();
        let restarted = recovered.stats(4, &BTreeSet::new(), i64::MAX);
        assert_eq!(restarted.message_count, 4);
        assert_eq!(restarted.requeue_count, 1);
        assert_eq!(restarted.timeout_count, 1);
    }

    #[test]
    fn legacy_checkpoint_starts_monotonic_count_at_its_persisted_barrier() {
        let checkpoint: ChannelCheckpoint = serde_json::from_value(serde_json::json!({
            "format": 7,
            "name": "workers",
            "barrier_position": 5,
            "ack_floor_position": 5,
            "acknowledged": [],
            "requeued_until": {},
            "paused": false,
            "ephemeral": false
        }))
        .unwrap();
        let mut state = ChannelState::from_checkpoint(checkpoint, 16).unwrap();
        assert_eq!(state.stats(7, &BTreeSet::new(), i64::MAX).message_count, 2);
        state.apply(&ChannelCommand::Empty {
            through_position: 7,
        });
        assert_eq!(state.stats(7, &BTreeSet::new(), i64::MAX).message_count, 2);
    }
}
