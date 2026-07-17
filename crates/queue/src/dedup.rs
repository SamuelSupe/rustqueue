use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) struct DedupKey {
    pub operation_id: u64,
    pub topic: String,
    pub partition: u16,
}

struct DedupValue {
    inserted_at: Instant,
    message_ids: Vec<u64>,
}

pub(crate) struct DedupCache {
    values: HashMap<DedupKey, DedupValue>,
    order: VecDeque<(Instant, DedupKey)>,
    max_entries: usize,
    ttl: Duration,
}

impl DedupCache {
    pub fn new(max_entries: usize, ttl: Duration) -> Self {
        Self {
            values: HashMap::new(),
            order: VecDeque::new(),
            max_entries: max_entries.max(1),
            ttl,
        }
    }

    pub fn get(&mut self, key: &DedupKey) -> Option<Vec<u64>> {
        self.expire(Instant::now());
        self.values.get(key).map(|value| value.message_ids.clone())
    }

    pub fn insert(&mut self, key: DedupKey, message_ids: Vec<u64>) {
        let now = Instant::now();
        self.values.insert(
            key.clone(),
            DedupValue {
                inserted_at: now,
                message_ids,
            },
        );
        self.order.push_back((now, key));
        self.expire(now);
    }

    fn expire(&mut self, now: Instant) {
        while self.values.len() > self.max_entries
            || self
                .order
                .front()
                .is_some_and(|(created, _)| now.duration_since(*created) >= self.ttl)
        {
            let Some((created, key)) = self.order.pop_front() else {
                break;
            };
            if self
                .values
                .get(&key)
                .is_some_and(|value| value.inserted_at == created)
            {
                self.values.remove(&key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(id: u64) -> DedupKey {
        DedupKey {
            operation_id: id,
            topic: "events".into(),
            partition: 0,
        }
    }

    #[test]
    fn cache_is_bounded_and_newest_value_wins() {
        let mut cache = DedupCache::new(2, Duration::from_secs(60));
        cache.insert(key(1), vec![1]);
        cache.insert(key(2), vec![2]);
        cache.insert(key(1), vec![3]);
        cache.insert(key(3), vec![4]);
        assert_eq!(cache.get(&key(1)), Some(vec![3]));
        assert!(cache.get(&key(2)).is_none());
        assert_eq!(cache.get(&key(3)), Some(vec![4]));
    }
}
