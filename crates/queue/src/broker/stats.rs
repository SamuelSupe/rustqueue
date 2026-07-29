use super::*;
use crate::model::{BrokerStats, QueueAggregateStats, TopicStats};

impl Broker {
    pub fn stats(&self) -> BrokerStats {
        self.filtered_stats(None, None)
    }

    pub fn filtered_stats(&self, topic: Option<&str>, channel: Option<&str>) -> BrokerStats {
        let mut topics = self.topic_stats(topic);
        if let Some(channel) = channel {
            for topic in &mut topics {
                topic.channels.retain(|candidate| candidate.name == channel);
            }
        }
        topics.sort_by(|left, right| left.name.cmp(&right.name));
        self.snapshot(topics)
    }

    pub fn metrics_stats(&self, detailed: bool, max_series: usize) -> BrokerStats {
        let handles: Vec<_> = self.inner.topics.read().values().cloned().collect();
        let mut aggregate = QueueAggregateStats::default();
        let mut topics = Vec::new();
        let mut remaining = max_series;
        for handle in handles {
            let _commit_gate = handle.commit_gate.lock();
            let mut topic = handle.state.lock();
            topic.add_aggregate_stats(&mut aggregate);
            if detailed && remaining >= 5 {
                let mut stats = topic.stats();
                remaining = remaining.saturating_sub(5);
                let channel_limit = remaining / 4;
                stats.channels.truncate(channel_limit);
                remaining = remaining.saturating_sub(stats.channels.len().saturating_mul(4));
                topics.push(stats);
            }
        }
        topics.sort_by(|left, right| left.name.cmp(&right.name));
        BrokerStats {
            aggregate,
            ..self.snapshot(topics)
        }
    }

    fn topic_stats(&self, topic: Option<&str>) -> Vec<TopicStats> {
        let handles: Vec<_> = match topic {
            Some(name) => self
                .inner
                .topics
                .read()
                .get(name)
                .cloned()
                .into_iter()
                .collect(),
            None => self.inner.topics.read().values().cloned().collect(),
        };
        handles
            .into_iter()
            .map(|topic| {
                let _commit_gate = topic.commit_gate.lock();
                topic.state.lock().stats()
            })
            .collect()
    }

    fn snapshot(&self, topics: Vec<TopicStats>) -> BrokerStats {
        let mut aggregate = QueueAggregateStats::default();
        for topic in &topics {
            aggregate.add_topic(topic);
        }
        BrokerStats {
            publish_group_commit: self.inner.publish_groups.stats(),
            channel_group_commit: self.inner.channel_groups.stats(),
            latency: self.inner.metrics.snapshot(),
            delivery_budget: self.inner.delivery_budget.snapshot(),
            aggregate,
            topics,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn filtered_stats_only_return_the_requested_topic_and_channel() {
        let root = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig {
            data_path: root.path().into(),
            ..BrokerConfig::default()
        })
        .unwrap();
        broker.create_channel("events", "workers").await.unwrap();
        broker.create_channel("events", "audit").await.unwrap();
        broker.create_channel("other", "workers").await.unwrap();

        let stats = broker.filtered_stats(Some("events"), Some("workers"));
        assert_eq!(stats.aggregate.topic_count, 1);
        assert_eq!(stats.aggregate.channel_count, 1);
        assert_eq!(stats.topics.len(), 1);
        assert_eq!(stats.topics[0].name, "events");
        assert_eq!(stats.topics[0].channels.len(), 1);
        assert_eq!(stats.topics[0].channels[0].name, "workers");

        assert!(broker
            .filtered_stats(Some("missing"), None)
            .topics
            .is_empty());
    }
}
