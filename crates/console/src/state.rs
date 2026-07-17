use crate::model::{RawCounters, Snapshot, TrendSample};
use std::collections::VecDeque;
use std::sync::{Mutex, RwLock};

pub struct LiveState {
    latest: RwLock<Option<Snapshot>>,
    history: Mutex<History>,
}

struct History {
    capacity: usize,
    samples: VecDeque<TrendSample>,
    previous: Option<RawCounters>,
}

impl LiveState {
    pub fn new(capacity: usize) -> Self {
        Self {
            latest: RwLock::new(None),
            history: Mutex::new(History {
                capacity: capacity.max(1),
                samples: VecDeque::with_capacity(capacity.max(1)),
                previous: None,
            }),
        }
    }

    pub fn publish(&self, mut snapshot: Snapshot, counters: RawCounters) {
        let mut history = self.history.lock().expect("history lock poisoned");
        let elapsed = history
            .previous
            .filter(|point| point.membership == counters.membership)
            .map(|point| counters.at_ms.saturating_sub(point.at_ms) as f64 / 1000.0)
            .unwrap_or_default();
        let rate = |current: u64, previous: u64| {
            if elapsed > 0.0 {
                current.saturating_sub(previous) as f64 / elapsed
            } else {
                0.0
            }
        };
        let previous = history.previous.unwrap_or(counters);
        snapshot.summary.publish_per_second =
            rate(counters.publish_messages, previous.publish_messages);
        snapshot.summary.deliver_per_second =
            rate(counters.delivered_messages, previous.delivered_messages);
        snapshot.summary.finish_per_second =
            rate(counters.finished_messages, previous.finished_messages);
        snapshot.summary.publish_bytes_per_second =
            rate(counters.publish_bytes, previous.publish_bytes);
        history.previous = Some(counters);
        history.samples.push_back(TrendSample {
            at_ms: counters.at_ms,
            publish_per_second: snapshot.summary.publish_per_second,
            deliver_per_second: snapshot.summary.deliver_per_second,
            finish_per_second: snapshot.summary.finish_per_second,
            publish_bytes_per_second: snapshot.summary.publish_bytes_per_second,
            depth: snapshot.summary.depth,
            in_flight: snapshot.summary.in_flight,
            disk_used_percent: snapshot.storage.used_percent,
        });
        while history.samples.len() > history.capacity {
            history.samples.pop_front();
        }
        snapshot.history = history.samples.iter().cloned().collect();
        *self.latest.write().expect("snapshot lock poisoned") = Some(snapshot);
    }

    pub fn snapshot(&self) -> Option<Snapshot> {
        self.latest.read().expect("snapshot lock poisoned").clone()
    }

    pub fn record_error(&self, detail: String) {
        if let Some(snapshot) = self
            .latest
            .write()
            .expect("snapshot lock poisoned")
            .as_mut()
        {
            snapshot.complete = false;
            snapshot.errors = vec![detail];
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn computes_rates_and_bounds_history() {
        let state = LiveState::new(2);
        for index in 0..3 {
            state.publish(
                Snapshot::default(),
                RawCounters {
                    at_ms: index * 1_000,
                    membership: 1,
                    publish_messages: index * 10,
                    ..Default::default()
                },
            );
        }
        let snapshot = state.snapshot().unwrap();
        assert_eq!(snapshot.history.len(), 2);
        assert_eq!(snapshot.summary.publish_per_second, 10.0);
    }
}
