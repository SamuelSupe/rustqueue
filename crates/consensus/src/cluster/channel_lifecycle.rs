use super::*;

const CHANNEL_TRANSITION_RETRIES: usize = 250;
const CHANNEL_TRANSITION_RETRY: std::time::Duration = std::time::Duration::from_millis(20);

impl ClusterRuntime {
    pub fn channel_is_active(&self, topic: &str, channel: &str) -> bool {
        self.metadata.channel_is_active(topic, channel)
    }

    pub fn active_partition_count(&self, topic: &str) -> usize {
        self.metadata
            .topic_route(topic)
            .map(|topic| topic.active_count())
            .unwrap_or_default()
    }

    pub(super) async fn prepare_channel_generation(
        &self,
        topic: &str,
        channel: &str,
    ) -> anyhow::Result<u64> {
        for _ in 0..CHANNEL_TRANSITION_RETRIES {
            let deleting = self
                .metadata
                .channel(topic, channel)
                .is_some_and(|channel| channel.state == ChannelLifecycle::Deleting);
            if !deleting {
                let response = self
                    .metadata_group()
                    .write(QueueCommand::PrepareChannel {
                        topic: topic.to_owned(),
                        channel: channel.to_owned(),
                    })
                    .await?;
                if response.error.is_none() {
                    if let Some(descriptor) = self
                        .metadata
                        .channel(topic, channel)
                        .filter(|channel| channel.state != ChannelLifecycle::Deleting)
                    {
                        return Ok(descriptor.generation);
                    }
                }
            }
            tokio::time::sleep(CHANNEL_TRANSITION_RETRY).await;
        }
        anyhow::bail!("channel lifecycle transition did not settle")
    }
}
