use super::*;

#[derive(Clone, Default)]
pub(super) struct EphemeralConsumers {
    counts: Arc<tokio::sync::Mutex<HashMap<(String, String), usize>>>,
}

impl EphemeralConsumers {
    pub async fn register(
        &self,
        broker: &Broker,
        topic: &str,
        channel: &str,
    ) -> Result<(), BrokerError> {
        let counts = Arc::clone(&self.counts);
        let broker = broker.clone();
        let topic = topic.to_owned();
        let channel = channel.to_owned();
        let (reply, result) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let mut counts = counts.lock().await;
            let outcome = broker.create_channel(&topic, &channel).await;
            complete_registration(&mut counts, &broker, &topic, &channel, outcome, reply).await;
        });
        let commit = result
            .await
            .unwrap_or(Err(BrokerError::StorageUnavailable))?;
        commit.send(()).map_err(|_| BrokerError::StorageUnavailable)
    }

    pub async fn unregister(&self, broker: &Broker, topic: &str, channel: &str) {
        let mut counts = self.counts.lock().await;
        let key = (topic.to_owned(), channel.to_owned());
        let Some(count) = counts.get_mut(&key) else {
            return;
        };
        *count = count.saturating_sub(1);
        if *count > 0 {
            return;
        }
        counts.remove(&key);
        let _ = broker.delete_channel(topic, channel).await;
    }
}

async fn complete_registration(
    counts: &mut HashMap<(String, String), usize>,
    broker: &Broker,
    topic: &str,
    channel: &str,
    outcome: Result<(), BrokerError>,
    reply: tokio::sync::oneshot::Sender<Result<tokio::sync::oneshot::Sender<()>, BrokerError>>,
) {
    if let Err(error) = outcome {
        let _ = reply.send(Err(error));
        return;
    }
    let key = (topic.to_owned(), channel.to_owned());
    *counts.entry(key.clone()).or_default() += 1;
    let (commit, committed) = tokio::sync::oneshot::channel();
    let accepted = reply.send(Ok(commit)).is_ok() && committed.await.is_ok();
    if accepted {
        return;
    }
    let remove = counts.get_mut(&key).is_some_and(|count| {
        *count = count.saturating_sub(1);
        *count == 0
    });
    if remove {
        counts.remove(&key);
        let _ = broker.delete_channel(topic, channel).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn disconnect_and_new_subscribe_cannot_delete_the_active_channel() {
        let root = tempdir().unwrap();
        let broker = Broker::open(rustqueue_queue::BrokerConfig {
            data_path: root.path().to_path_buf(),
            ..rustqueue_queue::BrokerConfig::default()
        })
        .unwrap();
        let consumers = EphemeralConsumers::default();

        consumers
            .register(&broker, "events", "live#ephemeral")
            .await
            .unwrap();
        let blocker = consumers.counts.lock().await;
        let unregister = {
            let broker = broker.clone();
            let consumers = consumers.clone();
            tokio::spawn(async move {
                consumers
                    .unregister(&broker, "events", "live#ephemeral")
                    .await;
            })
        };
        tokio::task::yield_now().await;
        let register = {
            let broker = broker.clone();
            let consumers = consumers.clone();
            tokio::spawn(async move {
                consumers
                    .register(&broker, "events", "live#ephemeral")
                    .await
            })
        };
        tokio::task::yield_now().await;
        drop(blocker);
        unregister.await.unwrap();
        register.await.unwrap().unwrap();

        assert_eq!(
            broker.channel_names("events").unwrap(),
            vec!["live#ephemeral"]
        );
    }

    #[tokio::test]
    async fn cancelled_subscribe_does_not_leave_an_orphaned_ephemeral_channel() {
        let root = tempdir().unwrap();
        let broker = Broker::open(rustqueue_queue::BrokerConfig {
            data_path: root.path().to_path_buf(),
            ..rustqueue_queue::BrokerConfig::default()
        })
        .unwrap();
        broker
            .create_channel("events", "live#ephemeral")
            .await
            .unwrap();
        let consumers = EphemeralConsumers::default();
        let mut counts = consumers.counts.lock().await;
        let (reply, result) = tokio::sync::oneshot::channel();
        drop(result);
        complete_registration(
            &mut counts,
            &broker,
            "events",
            "live#ephemeral",
            Ok(()),
            reply,
        )
        .await;

        assert!(counts.is_empty());
        assert!(broker.channel_names("events").unwrap().is_empty());
    }
}
