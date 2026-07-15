use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeadLetterReason {
    Attempts,
    Retention,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn dead_letter_if_needed(
    config: &Config,
    broker: &Broker,
    consensus: Option<&ClusterRuntime>,
    operation_ids: &AtomicU64,
    metrics: &Metrics,
    topic: &str,
    channel: &str,
    delivery: &RemoteDelivery,
) -> Result<bool, BrokerError> {
    let Some(reason) = reason(config, topic, delivery) else {
        return Ok(false);
    };
    let target = dead_letter_topic(topic, channel, &config.queue.dead_letter_suffix)
        .map_err(BrokerError::InvalidRecord)?;
    let result = match publish_messages(
        broker,
        consensus,
        operation_ids,
        &target,
        vec![delivery.body.clone()],
        Duration::ZERO,
    )
    .await
    {
        Ok(_) => finish_message(broker, consensus, topic, channel, delivery.id).await,
        Err(error) => Err(error),
    };
    if let Err(error) = result {
        release_failed_move(broker, consensus, topic, channel, delivery.id).await;
        return Err(error);
    }
    metrics.dead_letter_messages.fetch_add(1, Ordering::Relaxed);
    if reason == DeadLetterReason::Retention {
        metrics
            .retention_expired_messages
            .fetch_add(1, Ordering::Relaxed);
    }
    tracing::info!(
        source_topic = topic,
        source_channel = channel,
        target_topic = target,
        message_id = delivery.id,
        ?reason,
        "message moved to dead-letter topic"
    );
    Ok(true)
}

fn reason(config: &Config, topic: &str, delivery: &RemoteDelivery) -> Option<DeadLetterReason> {
    if topic.ends_with(&config.queue.dead_letter_suffix) {
        return None;
    }
    if delivery.attempts >= config.queue.max_delivery_attempts {
        return Some(DeadLetterReason::Attempts);
    }
    let retention = config.queue.message_retention_seconds;
    if retention == 0 {
        return None;
    }
    let age_ns = now_ns().saturating_sub(delivery.timestamp_ns).max(0) as u64;
    (age_ns >= retention.saturating_mul(1_000_000_000)).then_some(DeadLetterReason::Retention)
}

async fn release_failed_move(
    broker: &Broker,
    consensus: Option<&ClusterRuntime>,
    topic: &str,
    channel: &str,
    id: u64,
) {
    if let Some(consensus) = consensus {
        let _ = consensus
            .release(rustqueue_consensus::ReleaseRequest {
                topic: topic.to_owned(),
                channel: channel.to_owned(),
                message_ids: vec![id],
            })
            .await;
    } else {
        broker.release(topic, channel, &[id]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tempfile::tempdir;

    fn delivery(attempts: u16, timestamp_ns: i64) -> RemoteDelivery {
        RemoteDelivery {
            id: 1,
            timestamp_ns,
            attempts,
            body: Bytes::from_static(b"body"),
        }
    }

    #[test]
    fn policy_distinguishes_attempts_retention_and_dlq_recursion() {
        let mut config = Config::default();
        config.queue.max_delivery_attempts = 3;
        assert_eq!(
            reason(&config, "events", &delivery(3, now_ns())),
            Some(DeadLetterReason::Attempts)
        );
        config.queue.message_retention_seconds = 1;
        assert_eq!(
            reason(
                &config,
                "events",
                &delivery(1, now_ns().saturating_sub(2_000_000_000))
            ),
            Some(DeadLetterReason::Retention)
        );
        assert_eq!(reason(&config, "events.DLQ", &delivery(99, 0)), None);
    }

    #[test]
    fn long_names_get_a_stable_nsq_safe_dlq_name() {
        let topic = "a".repeat(64);
        let channel = "b".repeat(64);
        let name = dead_letter_topic(&topic, &channel, ".DLQ").unwrap();
        assert!(name.len() <= 64);
        assert!(rustqueue_protocol::validate_name(&name).is_ok());
        assert_eq!(name, dead_letter_topic(&topic, &channel, ".DLQ").unwrap());
    }

    #[tokio::test]
    async fn publishes_dlq_before_finishing_the_source_message() {
        let root = tempdir().unwrap();
        let broker = Broker::open(rustqueue_queue::BrokerConfig {
            data_path: root.path().to_path_buf(),
            ..rustqueue_queue::BrokerConfig::default()
        })
        .unwrap();
        broker.create_channel("events", "workers").unwrap();
        broker
            .publish(
                "events",
                vec![b"poison".to_vec()],
                Duration::ZERO,
                None,
                None,
            )
            .unwrap();
        let mut cursor = 0;
        let first = broker
            .next_message("events", "workers", &mut cursor, None)
            .await
            .unwrap()
            .unwrap();
        broker
            .requeue("events", "workers", first.id, Duration::ZERO)
            .unwrap();
        let second = broker
            .next_message("events", "workers", &mut cursor, None)
            .await
            .unwrap()
            .unwrap();
        let mut config = Config::default();
        config.queue.max_delivery_attempts = 2;
        let metrics = Metrics::default();
        assert!(dead_letter_if_needed(
            &config,
            &broker,
            None,
            &AtomicU64::new(1),
            &metrics,
            "events",
            "workers",
            &RemoteDelivery {
                id: second.id,
                timestamp_ns: second.timestamp_ns,
                attempts: second.attempts,
                body: bytes::Bytes::from_owner(second.body),
            },
        )
        .await
        .unwrap());

        let stats = broker.stats();
        let source = stats
            .topics
            .iter()
            .find(|topic| topic.name == "events")
            .unwrap();
        assert_eq!(source.partitions[0].channels[0].depth, 0);
        let dlq = stats
            .topics
            .iter()
            .find(|topic| topic.name == "events.workers.DLQ")
            .unwrap();
        assert_eq!(dlq.message_count, 1);
        assert_eq!(metrics.dead_letter_messages.load(Ordering::Relaxed), 1);
    }
}
