use super::*;
use futures::{stream, StreamExt, TryStreamExt};

const MAX_CONCURRENT_QUORUM_CHECKS: usize = 32;
const HEALTH_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(2);

#[derive(Default)]
pub(super) struct HealthCache {
    checked_at: Option<std::time::Instant>,
    error: Option<String>,
}

impl ClusterRuntime {
    pub async fn ensure_quorum_cached(&self) -> anyhow::Result<()> {
        let mut cache = self.health_cache.lock().await;
        if cache
            .checked_at
            .is_some_and(|checked_at| checked_at.elapsed() <= HEALTH_CACHE_TTL)
        {
            return cache
                .error
                .clone()
                .map_or(Ok(()), |error| Err(anyhow::anyhow!(error)));
        }
        let result = self
            .ensure_quorum()
            .await
            .map_err(|error| error.to_string());
        cache.checked_at = Some(std::time::Instant::now());
        cache.error = result.as_ref().err().cloned();
        result.map_err(anyhow::Error::msg)
    }

    pub async fn ensure_quorum(&self) -> anyhow::Result<()> {
        self.ensure_clock_safe().map_err(anyhow::Error::msg)?;
        self.metadata_group().ensure_quorum().await?;
        let partitions: Vec<_> = self
            .metadata
            .snapshot()
            .topics
            .into_values()
            .flat_map(|topic| topic.partitions)
            .filter(|partition| partition.lifecycle == crate::PartitionLifecycle::Active)
            .collect();
        let _: Vec<()> = stream::iter(partitions)
            .map(|partition| self.ensure_partition_quorum(partition))
            .buffer_unordered(MAX_CONCURRENT_QUORUM_CHECKS)
            .try_collect()
            .await?;
        Ok(())
    }

    async fn ensure_partition_quorum(&self, partition: PartitionDescriptor) -> anyhow::Result<()> {
        if self.leader_routes.prefers_local(&partition, self.node_id) {
            if let Some(group) = self.group(partition.group_key()).await {
                if self
                    .accept_routed(&partition, group.quorum_routed_local().await)?
                    .is_some()
                {
                    return Ok(());
                }
            }
        }
        self.post_to_leader::<_, ()>(
            &partition,
            "quorum",
            &(),
            crate::network_metrics::RpcKind::Control,
            INTERNAL_SMALL_FRAME_BYTES,
            INTERNAL_SMALL_FRAME_BYTES,
        )
        .await
    }
}
