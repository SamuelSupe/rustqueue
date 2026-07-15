use super::*;
use crate::PartitionLifecycle;
use futures::{stream, StreamExt};
use rustqueue_queue::{BrokerStats, PartitionStats, TopicStats};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

const MAX_CONCURRENT_STATS_REQUESTS: usize = 32;
const STATS_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(2);

#[derive(Default)]
pub(super) struct StatsCache {
    value: Option<(std::time::Instant, ClusterStats)>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GroupStatsResponse {
    pub group_id: crate::GlobalGroupId,
    pub topic: String,
    pub partition: PartitionStats,
}

#[derive(Clone, Debug, Serialize)]
pub struct ClusterStats {
    pub complete: bool,
    pub missing_groups: Vec<crate::GlobalGroupId>,
    pub collected_at_ms: u64,
    pub stats: BrokerStats,
}

impl ClusterRuntime {
    pub async fn local_group_stats(
        &self,
        group_id: crate::GlobalGroupId,
    ) -> anyhow::Result<GroupStatsResponse> {
        let group = self
            .partition_group(group_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("group {group_id} is not hosted locally"))?;
        if group.raft().metrics().borrow().current_leader != Some(self.node_id) {
            anyhow::bail!("group {group_id} is not led by this node");
        }
        let (topic, partition) = self
            .metadata
            .partition(group_id)
            .ok_or_else(|| anyhow::anyhow!("partition group {group_id} is not active"))?;
        let stats = self.broker.partition_stats(&topic, partition.number)?;
        Ok(GroupStatsResponse {
            group_id: partition.global_id(),
            topic,
            partition: stats,
        })
    }

    pub async fn cluster_stats(&self) -> ClusterStats {
        let mut cache = self.stats_cache.lock().await;
        if let Some((collected_at, stats)) = &cache.value {
            if collected_at.elapsed() <= STATS_CACHE_TTL {
                return stats.clone();
            }
        }
        let stats = self.collect_cluster_stats().await;
        cache.value = Some((std::time::Instant::now(), stats.clone()));
        stats
    }

    async fn collect_cluster_stats(&self) -> ClusterStats {
        let metadata = self.metadata.snapshot();
        let topics: BTreeMap<_, _> = metadata
            .topics
            .iter()
            .map(|(name, topic)| {
                (
                    name.clone(),
                    (
                        topic.paused,
                        topic
                            .channels
                            .values()
                            .filter(|channel| channel.state == ChannelLifecycle::Active)
                            .map(|channel| channel.name.clone())
                            .collect::<Vec<_>>(),
                    ),
                )
            })
            .collect();
        let client = self.client.clone();
        let nodes = self.nodes_snapshot();
        let partitions: Vec<_> = metadata
            .topics
            .values()
            .flat_map(|topic| &topic.partitions)
            .filter(|partition| partition.lifecycle == PartitionLifecycle::Active)
            .cloned()
            .collect();
        let mut requests = stream::iter(partitions.clone())
            .map(|partition| {
                let client = client.clone();
                let nodes = nodes.clone();
                async move {
                    let group_id = partition.global_id();
                    (group_id, fetch_group_stats(client, nodes, partition).await)
                }
            })
            .buffer_unordered(MAX_CONCURRENT_STATS_REQUESTS);

        let mut collected: BTreeMap<String, Vec<PartitionStats>> = BTreeMap::new();
        let mut collected_groups = BTreeSet::new();
        let mut missing = BTreeSet::new();
        while let Some((group_id, result)) = requests.next().await {
            match result {
                Ok(group) => {
                    collected_groups.insert(group_id);
                    collected
                        .entry(group.topic)
                        .or_default()
                        .push(group.partition);
                }
                Err(group_id) => {
                    missing.insert(group_id);
                }
            }
        }
        for group_id in partitions.iter().map(PartitionDescriptor::global_id) {
            if !collected_groups.contains(&group_id) {
                missing.insert(group_id);
            }
        }
        let mut output = Vec::new();
        for (name, mut partitions) in collected {
            partitions.sort_by_key(|partition| partition.partition);
            let (paused, channels) = topics.get(&name).cloned().unwrap_or_default();
            output.push(TopicStats {
                name,
                paused,
                message_count: partitions
                    .iter()
                    .map(|partition| partition.message_count)
                    .sum(),
                partitions,
                channels,
            });
        }
        output.sort_by(|left, right| left.name.cmp(&right.name));
        ClusterStats {
            complete: missing.is_empty(),
            missing_groups: missing.into_iter().collect(),
            collected_at_ms: wall_time_ms(),
            stats: BrokerStats { topics: output },
        }
    }
}

async fn fetch_group_stats(
    client: reqwest::Client,
    nodes: BTreeMap<NodeId, BasicNode>,
    partition: PartitionDescriptor,
) -> Result<GroupStatsResponse, crate::GlobalGroupId> {
    let mut replicas: Vec<_> = partition.replicas.iter().copied().collect();
    replicas.sort_by_key(|node| usize::from(Some(*node) != partition.leader_hint));
    for node_id in replicas {
        let Some(node) = nodes.get(&node_id) else {
            continue;
        };
        let response = client
            .get(format!(
                "{}/raft/groups/{}/stats",
                node.addr.trim_end_matches('/'),
                partition.group_key()
            ))
            .send()
            .await;
        let Ok(response) = response else { continue };
        let Ok(response) = response.error_for_status() else {
            continue;
        };
        if let Ok(stats) = response.json().await {
            return Ok(stats);
        }
    }
    Err(partition.global_id())
}
