use super::*;
use crate::{OperationState, PartitionLifecycle, TopicState};

impl ClusterRuntime {
    pub(super) async fn reconcile_disk_pressure(&self) -> anyhow::Result<usize> {
        let now_ms = wall_time_ms();
        if self.disk_status()?.eligible {
            self.disk_pressure_since_ms.store(0, Ordering::Release);
            return Ok(0);
        }
        let since = self.disk_pressure_since_ms.load(Ordering::Acquire);
        if since == 0 {
            let _ = self.disk_pressure_since_ms.compare_exchange(
                0,
                now_ms,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            return Ok(0);
        }
        let grace_ms = self
            .automation
            .disk_pressure_grace_seconds
            .saturating_mul(1_000);
        if now_ms.saturating_sub(since) < grace_ms {
            return Ok(0);
        }

        let snapshot = self.metadata.snapshot();
        let mut partitions: Vec<_> = snapshot
            .topics
            .values()
            .filter(|topic| topic.state == TopicState::Active)
            .flat_map(|topic| {
                topic
                    .partitions
                    .iter()
                    .filter(|partition| {
                        partition.lifecycle == PartitionLifecycle::Active
                            && partition.replicas.contains(&self.node_id)
                    })
                    .map(|partition| (topic.name.clone(), partition.clone()))
            })
            .collect();
        partitions.sort_by_key(|(_, partition)| partition.global_id());
        if partitions.is_empty() {
            return Ok(0);
        }
        let start = self.disk_gc_cursor.fetch_add(1, Ordering::Relaxed) as usize % partitions.len();
        let (topic, partition) = &partitions[start];
        let Some(group) = self.group(partition.group_key()).await else {
            return Ok(0);
        };

        // First release ordinary ACKed payload and purged Raft segments. Only
        // if the node remains pressured do we consider destructive eviction.
        let mut actions = usize::from(
            group
                .compact_partition_storage(topic, partition.number)
                .await?
                > 0,
        );
        if self.disk_status()?.eligible {
            self.disk_pressure_since_ms.store(0, Ordering::Release);
            return Ok(actions);
        }
        if !self.automation.protective_eviction_enabled
            || self.active_feature_level() < crate::FEATURE_LEVEL_PROTECTIVE_EVICTION
            || !no_available_storage_target(&snapshot, now_ms as i64)
            || has_active_membership_work(&snapshot)
            || group.raft().metrics().borrow().current_leader != Some(self.node_id)
        {
            return Ok(actions);
        }
        let Some(candidate) = self
            .broker
            .protective_eviction_candidate(topic, partition.number)?
        else {
            return Ok(actions);
        };
        let operation_id = ((self.node_id & 0xffff) << 48) | (now_ms & ((1u64 << 48) - 1));
        let response = self
            .write_partition(
                partition,
                QueueCommand::ProtectiveEvict {
                    operation_id,
                    topic: topic.clone(),
                    partition: partition.number,
                    through_message_id: candidate.through_message_id,
                },
            )
            .await?;
        ensure_response(&response)?;
        self.protective_evicted_messages
            .fetch_add(candidate.message_count as u64, Ordering::Relaxed);
        self.protective_evicted_bytes
            .fetch_add(candidate.payload_bytes, Ordering::Relaxed);
        tracing::warn!(
            audit = true,
            destructive = true,
            operation_id,
            group_id = %partition.global_id(),
            topic,
            partition = partition.number,
            through_message_id = candidate.through_message_id,
            messages = candidate.message_count,
            payload_bytes = candidate.payload_bytes,
            reason = "cluster_disk_pressure",
            "quorum committed protective segment eviction"
        );
        group
            .compact_partition_storage(topic, partition.number)
            .await?;
        actions += 1;
        if self.disk_status()?.eligible {
            self.disk_pressure_since_ms.store(0, Ordering::Release);
        }
        Ok(actions)
    }
}

fn no_available_storage_target(metadata: &crate::ClusterMetadata, now_ms: i64) -> bool {
    !metadata.nodes.keys().any(|node_id| {
        !metadata.drained_nodes.contains(node_id)
            && metadata
                .maintenance_nodes
                .get(node_id)
                .is_none_or(|lease| lease.expires_at_ms <= now_ms)
            && metadata
                .node_health
                .get(node_id)
                .is_some_and(|health| health.available && health.storage_eligible)
    })
}

fn has_active_membership_work(metadata: &crate::ClusterMetadata) -> bool {
    metadata.operations.values().any(|operation| {
        matches!(operation.state, OperationState::Running)
            && !matches!(
                operation.kind,
                crate::OperationKind::ExpandPartitions { .. }
            )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog() -> MetadataCatalog {
        let nodes = (1..=3)
            .map(|id| {
                (
                    id,
                    crate::NodeDescriptor {
                        id,
                        raft_address: format!("https://node-{id}:4250"),
                        broadcast_address: format!("node-{id}"),
                        tcp_port: 4150,
                        http_port: 4151,
                        tls_server_name: format!("node-{id}"),
                        failure_domain: format!("zone-{id}"),
                        peer_id: None,
                        cell_id: crate::CellId::BOOTSTRAP,
                        federation_router: false,
                    },
                )
            })
            .collect();
        MetadataCatalog::new(nodes, 1, 3).unwrap()
    }

    #[test]
    fn eviction_requires_every_available_target_to_be_under_pressure() {
        let catalog = catalog();
        for node in 1..=3 {
            catalog
                .observe_node_health(node, true, 90, 1, false, 1_000)
                .unwrap();
        }
        assert!(no_available_storage_target(&catalog.snapshot(), 1_000));

        catalog
            .observe_node_health(3, true, 50, u64::MAX, true, 2_000)
            .unwrap();
        assert!(!no_available_storage_target(&catalog.snapshot(), 2_000));
    }
}
