use super::*;
use futures::{stream, StreamExt, TryStreamExt};

const MAX_CONCURRENT_GROUP_PURGES: usize = 8;

impl ClusterRuntime {
    pub(super) async fn delete_topic_durable(
        &self,
        topic_name: &str,
    ) -> anyhow::Result<QueueResponse> {
        if self.control_plane_enabled() {
            return self.delete_topic_federated(topic_name).await;
        }
        self.delete_topic_cell_local(topic_name).await
    }

    pub(super) async fn delete_topic_cell_local(
        &self,
        topic_name: &str,
    ) -> anyhow::Result<QueueResponse> {
        let _guard = self.topic_delete_lock.lock().await;
        let Some(mut topic) = self.metadata.topic(topic_name) else {
            return Ok(QueueResponse::default());
        };
        if topic.state != crate::TopicState::Deleting {
            let response = self
                .metadata_group()
                .write(QueueCommand::PrepareDeleteTopic {
                    topic: topic_name.to_owned(),
                })
                .await?;
            ensure_response(&response)?;
            topic = self
                .metadata
                .topic(topic_name)
                .ok_or_else(|| anyhow::anyhow!("topic disappeared during deletion"))?;
        }

        stream::iter(topic.partitions)
            .flat_map(|partition| {
                let group_id = partition.global_id();
                stream::iter(
                    partition
                        .replicas
                        .into_iter()
                        .map(move |node_id| (group_id, node_id)),
                )
            })
            .map(|(group_id, node_id)| self.purge_replica(group_id, node_id))
            .buffer_unordered(MAX_CONCURRENT_GROUP_PURGES)
            .try_collect::<Vec<_>>()
            .await?;

        let response = self
            .metadata_group()
            .write(QueueCommand::CompleteDeleteTopic {
                topic: topic_name.to_owned(),
            })
            .await?;
        ensure_response(&response)?;
        tracing::info!(topic = topic_name, "topic groups and metadata were deleted");
        Ok(response)
    }

    pub(super) async fn reconcile_topic_deletions(&self) -> anyhow::Result<usize> {
        let topics = self.metadata.deleting_topics();
        let count = topics.len();
        for topic in topics {
            self.delete_topic_durable(&topic.name).await?;
        }
        Ok(count)
    }
}
