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
    let result = broker
        .move_to_dead_letter(topic, channel, delivery.id, &target, delivery.body.clone())
        .await;
    if let Err(error) = result {
        broker.release(topic, channel, &[delivery.id]);
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

fn dead_letter_topic(topic: &str, channel: &str, suffix: &str) -> Result<String, String> {
    if suffix.is_empty()
        || suffix.len() > 16
        || !suffix
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err("dead-letter suffix is invalid".into());
    }
    let candidate = format!("{topic}.{channel}{suffix}");
    if rustqueue_protocol::validate_name(&candidate).is_ok() {
        return Ok(candidate);
    }
    let hash = crc32c::crc32c(format!("{topic}\0{channel}").as_bytes());
    let tail = format!(".{hash:08x}{suffix}");
    let keep = 64usize.saturating_sub(tail.len()).min(topic.len());
    let candidate = format!("{}{}", &topic[..keep], tail);
    rustqueue_protocol::validate_name(&candidate)
        .map_err(|_| "cannot derive a valid dead-letter topic name".to_owned())?;
    Ok(candidate)
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
        broker.create_channel("events", "workers").await.unwrap();
        broker
            .publish("events", vec![b"poison".to_vec()], Duration::ZERO)
            .await
            .unwrap();
        let first = broker
            .next_message("events", "workers", None)
            .await
            .unwrap()
            .unwrap();
        broker
            .requeue("events", "workers", first.id, Duration::ZERO)
            .await
            .unwrap();
        let second = broker
            .next_message("events", "workers", None)
            .await
            .unwrap()
            .unwrap();
        let mut config = Config::default();
        config.queue.max_delivery_attempts = 2;
        let metrics = Metrics::default();
        assert!(dead_letter_if_needed(
            &config,
            &broker,
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
        assert_eq!(source.channels[0].depth, 0);
        let dlq = stats
            .topics
            .iter()
            .find(|topic| topic.name == "events.workers.DLQ")
            .unwrap();
        assert_eq!(dlq.message_count, 1);
        assert_eq!(metrics.dead_letter_messages.load(Ordering::Relaxed), 1);
    }
}
