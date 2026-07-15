use super::*;
use crate::network_metrics::RpcKind;
use futures::{stream, StreamExt, TryStreamExt};

const MAX_CONCURRENT_TOPIC_WRITES: usize = 32;

impl ClusterRuntime {
    pub(super) async fn broadcast_topic(
        &self,
        topic: &str,
        command: QueueCommand,
    ) -> anyhow::Result<QueueResponse> {
        self.broadcast_topic_matching(topic, command, participates_in_topic_broadcast)
            .await
    }

    pub(super) async fn broadcast_channel_barrier(
        &self,
        topic: &str,
        command: QueueCommand,
    ) -> anyhow::Result<QueueResponse> {
        self.broadcast_topic_matching(topic, command, participates_in_channel_barrier)
            .await
    }

    async fn broadcast_topic_matching(
        &self,
        topic: &str,
        command: QueueCommand,
        include: fn(&PartitionDescriptor) -> bool,
    ) -> anyhow::Result<QueueResponse> {
        let descriptor = self
            .metadata
            .topic(topic)
            .ok_or_else(|| anyhow::anyhow!("topic not found"))?;
        let partitions: Vec<_> = descriptor
            .partitions
            .iter()
            .filter(|partition| include(partition))
            .cloned()
            .collect();
        let results = stream::iter(partitions)
            .map(|partition| {
                let command = command.clone();
                async move {
                    let response = self.write_partition(&partition, command).await?;
                    ensure_response(&response)?;
                    Ok::<_, anyhow::Error>(response)
                }
            })
            .buffer_unordered(MAX_CONCURRENT_TOPIC_WRITES)
            .try_collect()
            .await?;
        Ok(QueueResponse {
            message_ids: Vec::new(),
            error: None,
            results,
        })
    }

    pub(super) async fn write_partition(
        &self,
        partition: &PartitionDescriptor,
        command: QueueCommand,
    ) -> anyhow::Result<QueueResponse> {
        self.write_partition_kind(partition, command, RpcKind::Write)
            .await
    }

    pub(super) async fn write_partition_kind(
        &self,
        partition: &PartitionDescriptor,
        command: QueueCommand,
        kind: RpcKind,
    ) -> anyhow::Result<QueueResponse> {
        if self.leader_routes.prefers_local(partition, self.node_id) {
            if let Some(group) = self.group(partition.group_key()).await {
                if let Some(response) =
                    self.accept_routed(partition, group.write_routed_local(command.clone()).await)?
                {
                    return Ok(response);
                }
            }
        }
        let envelope = crate::CommandEnvelope::new(command);
        self.post_to_leader(
            partition,
            "write",
            &envelope,
            kind,
            INTERNAL_WRITE_FRAME_BYTES,
            INTERNAL_WRITE_RESPONSE_BYTES,
        )
        .await
    }

    pub(super) async fn fetch_partition(
        &self,
        partition: &PartitionDescriptor,
        request: FetchRequest,
    ) -> anyhow::Result<FetchResponse> {
        if self.leader_routes.prefers_local(partition, self.node_id) {
            if let Some(group) = self.group(partition.group_key()).await {
                if let Some(response) =
                    self.accept_routed(partition, group.fetch_routed_local(request.clone()).await)?
                {
                    return Ok(response);
                }
            }
        }
        self.post_to_leader(
            partition,
            "fetch",
            &request,
            RpcKind::Fetch,
            INTERNAL_SMALL_FRAME_BYTES,
            INTERNAL_FETCH_RESPONSE_BYTES,
        )
        .await
    }
    pub(super) async fn wait_partition_ready(
        &self,
        partition: &PartitionDescriptor,
        request: FetchRequest,
    ) -> anyhow::Result<bool> {
        if self.leader_routes.prefers_local(partition, self.node_id) {
            if let Some(group) = self.group(partition.group_key()).await {
                if let Some(response) =
                    self.accept_routed(partition, group.ready_routed_local(request.clone()).await)?
                {
                    return Ok(response);
                }
            }
        }
        self.post_to_leader(
            partition,
            "ready",
            &request,
            RpcKind::Fetch,
            INTERNAL_SMALL_FRAME_BYTES,
            INTERNAL_SMALL_FRAME_BYTES,
        )
        .await
    }

    pub(super) async fn post_to_replicas<Req, Resp>(
        &self,
        partition: &PartitionDescriptor,
        operation: &str,
        request: &Req,
    ) -> anyhow::Result<Resp>
    where
        Req: Serialize + ?Sized,
        Resp: serde::de::DeserializeOwned,
    {
        let _timer = self.forward_latency.timer();
        let mut last_error = None;
        for node_id in &partition.replicas {
            let Some(node) = self.node(*node_id) else {
                continue;
            };
            match crate::post_binary_limited(
                &self.client,
                format!(
                    "{}/raft/groups/{}/{}",
                    node.addr.trim_end_matches('/'),
                    partition.group_key(),
                    operation
                ),
                request,
                INTERNAL_SMALL_FRAME_BYTES,
                INTERNAL_SMALL_FRAME_BYTES,
            )
            .await
            {
                Ok(response) => return Ok(response),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("partition has no reachable replicas")))
    }

    pub(super) fn partition_for_message(
        &self,
        topic: &str,
        message_id: u64,
    ) -> anyhow::Result<PartitionDescriptor> {
        let slot = (message_id >> 48) as u16;
        self.metadata
            .topic_route(topic)
            .and_then(|topic| topic.partition_by_slot(slot))
            .map(|partition| (*partition).clone())
            .ok_or_else(|| anyhow::anyhow!("message partition is not present in metadata"))
    }
}

fn participates_in_topic_broadcast(partition: &PartitionDescriptor) -> bool {
    partition.lifecycle != crate::PartitionLifecycle::Retired
}

fn participates_in_channel_barrier(partition: &PartitionDescriptor) -> bool {
    participates_in_topic_broadcast(partition)
        && (partition.lifecycle != crate::PartitionLifecycle::Preparing
            || partition.origin_cell == partition.home_cell)
}

#[cfg(test)]
pub(super) fn select_partition(
    topic: &crate::TopicDescriptor,
    operation_id: u64,
    requested: Option<u16>,
    routing_key: Option<&[u8]>,
) -> anyhow::Result<usize> {
    let partitions: Vec<_> = topic
        .partitions
        .iter()
        .filter(|partition| partition.lifecycle == crate::PartitionLifecycle::Active)
        .collect();
    if partitions.is_empty() {
        anyhow::bail!("topic has no partitions");
    }
    if let Some(number) = requested {
        return partitions
            .iter()
            .position(|partition| partition.number == number)
            .ok_or_else(|| anyhow::anyhow!("partition not found"));
    }
    if let Some(key) = routing_key {
        let slot =
            topic.key_routing_slots[crc32c::crc32c(key) as usize % topic.key_routing_slots.len()];
        return partitions
            .iter()
            .position(|partition| partition.slot == slot)
            .ok_or_else(|| anyhow::anyhow!("key routing partition is not active"));
    }
    Ok(operation_id as usize % partitions.len())
}

pub(super) fn partition_initializer(partition: &PartitionDescriptor) -> NodeId {
    let index = partition.number as usize % partition.replicas.len();
    *partition
        .replicas
        .iter()
        .nth(index)
        .expect("partition replica set is non-empty")
}

pub(super) fn command_topic(command: &QueueCommand) -> Option<&str> {
    match command {
        QueueCommand::CreateChannel { topic, .. }
        | QueueCommand::DeleteChannel { topic, .. }
        | QueueCommand::EmptyTopic { topic }
        | QueueCommand::EmptyChannel { topic, .. }
        | QueueCommand::PauseChannel { topic, .. } => Some(topic),
        _ => None,
    }
}

pub(super) fn command_message(command: &QueueCommand) -> Option<(&str, u64)> {
    match command {
        QueueCommand::Finish {
            topic, message_id, ..
        }
        | QueueCommand::Requeue {
            topic, message_id, ..
        } => Some((topic, *message_id)),
        _ => None,
    }
}

pub(super) fn ensure_response(response: &QueueResponse) -> anyhow::Result<()> {
    response
        .error
        .as_ref()
        .map_or(Ok(()), |error| Err(anyhow::anyhow!(error.to_owned())))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic() -> crate::TopicDescriptor {
        let partitions: Vec<_> = (0..4)
            .map(|number| PartitionDescriptor {
                group_id: number as u64 + 1,
                origin_cell: crate::CellId::BOOTSTRAP,
                number,
                slot: number + 1,
                replication_factor: 3,
                replicas: BTreeSet::from([1, 2, 3]),
                leader_hint: None,
                lifecycle: crate::PartitionLifecycle::Active,
                operation_id: None,
                home_cell: crate::CellId::BOOTSTRAP,
                wire_incarnation: 1,
            })
            .collect();
        crate::TopicDescriptor {
            name: "events".into(),
            state: crate::TopicState::Active,
            replication_factor: 3,
            key_routing_slots: partitions.iter().map(|partition| partition.slot).collect(),
            partitions,
            channels: BTreeMap::new(),
            next_channel_generation: 1,
            paused: false,
            topology_generation: 1,
            channel_catalog_revision: 0,
        }
    }

    #[test]
    fn routing_key_is_stable_and_explicit_partition_wins() {
        let topic = topic();
        let selected = select_partition(&topic, 99, None, Some(b"account-42")).unwrap();
        for operation in 0..100 {
            assert_eq!(
                select_partition(&topic, operation, None, Some(b"account-42")).unwrap(),
                selected
            );
        }
        assert_eq!(
            select_partition(&topic, 99, Some(3), Some(b"account-42")).unwrap(),
            3
        );
    }

    #[test]
    fn round_robin_seed_selects_exactly_one_partition() {
        let topic = topic();
        for operation in 0..100 {
            assert_eq!(
                select_partition(&topic, operation, None, None).unwrap(),
                operation as usize % topic.partitions.len()
            );
        }
    }

    #[test]
    fn channel_barrier_skips_only_a_preparing_migration_target() {
        let mut partition = topic().partitions.remove(0);
        partition.lifecycle = crate::PartitionLifecycle::Preparing;
        partition.origin_cell = crate::CellId(1);
        partition.home_cell = crate::CellId(2);
        assert!(!participates_in_channel_barrier(&partition));

        partition.home_cell = partition.origin_cell;
        assert!(participates_in_channel_barrier(&partition));

        partition.home_cell = crate::CellId(2);
        partition.lifecycle = crate::PartitionLifecycle::Active;
        assert!(participates_in_channel_barrier(&partition));
    }
}
