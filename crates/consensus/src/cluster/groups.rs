use super::*;

impl ClusterRuntime {
    pub async fn ensure_partition_local(&self, request: EnsureGroupRequest) -> anyhow::Result<()> {
        if !request.partition.replicas.contains(&self.node_id) {
            anyhow::bail!("node is not assigned to partition group");
        }
        if self.group(request.partition.group_key()).await.is_some() {
            return Ok(());
        }
        let _guard = self.ensure_lock.lock().await;
        if self.group(request.partition.group_key()).await.is_some() {
            return Ok(());
        }
        let topic = self
            .metadata
            .topic(&request.topic)
            .ok_or_else(|| anyhow::anyhow!("topic metadata has not reached this node"))?;
        if topic.state == crate::TopicState::Deleting {
            anyhow::bail!("topic is being deleted");
        }
        let layouts: Vec<_> = topic
            .partitions
            .iter()
            .map(|partition| rustqueue_queue::PartitionLayout {
                number: partition.number,
                slot: partition.slot,
                cell_id: partition.origin_cell.0,
                group_id: partition.group_id,
                wire_incarnation: partition.wire_incarnation,
            })
            .collect();
        self.broker
            .ensure_topic_layout_v4(&request.topic, &layouts, &topic.key_routing_slots)?;
        let nodes = self.nodes_snapshot();
        let group_key = request.partition.group_key();
        let group = ConsensusNode::open_group(
            group_key,
            self.node_id,
            &format!("{}-{group_key}", self.cluster_name),
            nodes,
            self.directory
                .join("groups")
                .join(group_key.storage_component()),
            Arc::clone(&self.broker),
            Arc::clone(&self.metadata),
            Network::for_group_with_snapshot(
                self.client.clone(),
                self.snapshot_client.clone(),
                group_key,
            ),
            StateMachineRole::Partition {
                topic: request.topic,
                partition: request.partition.number,
            },
        )
        .await?;
        self.groups.write().await.insert(group_key, group);
        Ok(())
    }

    pub async fn initialize_group_local(
        &self,
        group_id: crate::GlobalGroupId,
        voters: BTreeSet<NodeId>,
    ) -> anyhow::Result<()> {
        let group = self
            .partition_group(group_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("group {group_id} is not hosted locally"))?;
        if group
            .raft()
            .metrics()
            .borrow()
            .membership_config
            .voter_ids()
            .next()
            .is_some()
        {
            return Ok(());
        }
        let members = voters
            .iter()
            .map(|id| {
                self.node(*id)
                    .map(|node| (*id, node))
                    .ok_or_else(|| anyhow::anyhow!("voter {id} is unknown"))
            })
            .collect::<anyhow::Result<_>>()?;
        match group.initialize(members).await {
            Ok(()) => Ok(()),
            Err(error) if error.to_string().contains("already initialized") => Ok(()),
            Err(error) => {
                for _ in 0..20 {
                    if group
                        .raft()
                        .metrics()
                        .borrow()
                        .membership_config
                        .voter_ids()
                        .next()
                        .is_some()
                    {
                        return Ok(());
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(error.into())
            }
        }
    }

    pub(super) async fn ensure_topic(
        &self,
        topic: &str,
        partitions: Option<u16>,
        replication_factor: Option<u8>,
    ) -> anyhow::Result<()> {
        if self.metadata.topic_route(topic).is_some() {
            self.sync_catalog_topic(topic).await?;
            return Ok(());
        }
        let response = self
            .metadata_group()
            .write(QueueCommand::CreateTopic {
                topic: topic.to_owned(),
                partitions,
                replication_factor,
            })
            .await?;
        ensure_response(&response)?;
        self.ensure_topic_groups(topic).await?;
        self.sync_catalog_topic(topic).await
    }

    pub(super) async fn ensure_topic_groups(&self, topic: &str) -> anyhow::Result<()> {
        let descriptor = self
            .metadata
            .topic(topic)
            .ok_or_else(|| anyhow::anyhow!("topic metadata is unavailable"))?;
        for partition in descriptor
            .partitions
            .iter()
            .filter(|partition| partition.lifecycle != crate::PartitionLifecycle::Retired)
        {
            self.ensure_partition_group(topic, partition).await?;
        }
        Ok(())
    }

    pub(super) async fn ensure_partition_group(
        &self,
        topic: &str,
        partition: &PartitionDescriptor,
    ) -> anyhow::Result<()> {
        let request = EnsureGroupRequest {
            topic: topic.to_owned(),
            partition: partition.clone(),
        };
        for node_id in &partition.replicas {
            if *node_id == self.node_id {
                let mut last_error = None;
                for _ in 0..50 {
                    match self.ensure_partition_local(request.clone()).await {
                        Ok(()) => {
                            last_error = None;
                            break;
                        }
                        Err(error) => last_error = Some(error),
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
                if let Some(error) = last_error {
                    return Err(error);
                }
            } else {
                let node = self
                    .node(*node_id)
                    .ok_or_else(|| anyhow::anyhow!("replica node {node_id} is not configured"))?;
                let url = format!("{}/raft/groups/ensure", node.addr.trim_end_matches('/'));
                let mut last_error = None;
                for _ in 0..50 {
                    match self.client.post(&url).json(&request).send().await {
                        Ok(response) => match response.error_for_status() {
                            Ok(_) => {
                                last_error = None;
                                break;
                            }
                            Err(error) => last_error = Some(error.into()),
                        },
                        Err(error) => last_error = Some(error.into()),
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
                if let Some(error) = last_error {
                    return Err(error);
                }
            }
        }
        let initializer = partition_initializer(partition);
        let initialize = InitializeGroupRequest {
            voters: partition.replicas.clone(),
        };
        if initializer == self.node_id {
            self.initialize_group_local(partition.global_id(), initialize.voters)
                .await?;
        } else {
            let node = self.node(initializer).expect("replica is configured");
            self.client
                .post(format!(
                    "{}/raft/groups/{}/initialize",
                    node.addr.trim_end_matches('/'),
                    partition.group_key()
                ))
                .json(&initialize)
                .send()
                .await?
                .error_for_status()?;
        }
        self.wait_group_leader(partition).await
    }

    pub(super) async fn wait_group_leader(
        &self,
        partition: &PartitionDescriptor,
    ) -> anyhow::Result<()> {
        for _ in 0..50 {
            if let Some(group) = self.group(partition.group_key()).await {
                if group.raft().metrics().borrow().current_leader.is_some() {
                    return Ok(());
                }
            }
            for node_id in &partition.replicas {
                let Some(node) = self.node(*node_id) else {
                    continue;
                };
                let response = self
                    .client
                    .get(format!(
                        "{}/raft/groups/{}/health",
                        node.addr.trim_end_matches('/'),
                        partition.group_key()
                    ))
                    .send()
                    .await;
                if let Ok(response) = response {
                    if response.status().is_success() {
                        let health: serde_json::Value = response.json().await?;
                        if !health["current_leader"].is_null() {
                            return Ok(());
                        }
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        anyhow::bail!(
            "partition group {} did not elect a leader",
            partition.global_id()
        )
    }

    pub(super) async fn ensure_replica_host(
        &self,
        topic: &str,
        partition: &PartitionDescriptor,
        node_id: NodeId,
    ) -> anyhow::Result<()> {
        let request = EnsureGroupRequest {
            topic: topic.to_owned(),
            partition: partition.clone(),
        };
        if node_id == self.node_id {
            return self.ensure_partition_local(request).await;
        }
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow::anyhow!("replica node {node_id} is not configured"))?;
        self.client
            .post(format!(
                "{}/raft/groups/ensure",
                node.addr.trim_end_matches('/')
            ))
            .json(&request)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    pub async fn evacuate_local_leaders(&self) {
        let groups: Vec<_> = self.groups.read().await.values().cloned().collect();
        for group in groups {
            let metrics = group.raft().metrics().borrow().clone();
            if metrics.current_leader != Some(self.node_id) {
                continue;
            }
            if let Some(target) = metrics
                .membership_config
                .voter_ids()
                .find(|candidate| *candidate != self.node_id)
            {
                let _ = group.transfer_leadership(target).await;
            }
        }
    }

    pub(super) async fn wait_for_leader_change(
        &self,
        group: &ConsensusNode,
        previous: NodeId,
    ) -> anyhow::Result<()> {
        for _ in 0..100 {
            let metrics = group.raft().metrics().borrow().clone();
            let voters: BTreeSet<_> = metrics.membership_config.voter_ids().collect();
            if metrics
                .current_leader
                .is_some_and(|leader| leader != previous && voters.contains(&leader))
            {
                return Ok(());
            }
            for node_id in voters.iter().copied().filter(|node| *node != previous) {
                let Some(node) = self.node(node_id) else {
                    continue;
                };
                let response = self
                    .client
                    .get(format!(
                        "{}/raft/groups/{}/health",
                        node.addr.trim_end_matches('/'),
                        group.group_key()
                    ))
                    .send()
                    .await;
                let Ok(response) = response else { continue };
                if !response.status().is_success() {
                    continue;
                }
                let health: serde_json::Value = response.json().await?;
                if health["current_leader"]
                    .as_u64()
                    .is_some_and(|leader| leader != previous && voters.contains(&leader))
                {
                    return Ok(());
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        anyhow::bail!(
            "group {} did not elect a replacement for node {previous}",
            group.group_key()
        )
    }

    pub(super) async fn retire_replica(
        &self,
        group_id: crate::GlobalGroupId,
        node_id: NodeId,
    ) -> anyhow::Result<()> {
        let group_key = self.partition_group_key(group_id)?;
        if node_id == self.node_id {
            return self.retire_replica_key_local(group_key).await;
        }
        let node = self.node(node_id).expect("replica is configured");
        let response: OperationResponse = self
            .client
            .post(format!(
                "{}/raft/groups/{group_key}/retire",
                node.addr.trim_end_matches('/')
            ))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        response
            .error
            .map_or(Ok(()), |error| Err(anyhow::anyhow!(error)))
    }

    pub async fn retire_replica_local(&self, group_id: crate::GlobalGroupId) -> anyhow::Result<()> {
        let group_key = self.partition_group_key(group_id)?;
        self.retire_replica_key_local(group_key).await
    }

    pub async fn retire_replica_key_local(&self, group_key: GroupKey) -> anyhow::Result<()> {
        if !matches!(group_key, GroupKey::Partition(_)) {
            anyhow::bail!("control-plane group cannot be retired while the process is running");
        }
        let Some(group) = self.groups.write().await.remove(&group_key) else {
            return Ok(());
        };
        group.raft().shutdown().await?;
        let component = group_key.storage_component();
        let source = self.directory.join("groups").join(&component);
        let retired = self.directory.join("retired");
        crate::store::blocking_io::run(move || {
            if source.exists() {
                std::fs::create_dir_all(&retired)?;
                let stamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis();
                std::fs::rename(source, retired.join(format!("group-{component}-{stamp}")))?;
            }
            Ok(())
        })
        .await?;
        Ok(())
    }

    pub(super) async fn purge_replica(
        &self,
        group_id: crate::GlobalGroupId,
        node_id: NodeId,
    ) -> anyhow::Result<()> {
        let group_key = self.partition_group_key(group_id)?;
        if node_id == self.node_id {
            return self.purge_replica_key_local(group_key).await;
        }
        let node = self
            .node(node_id)
            .ok_or_else(|| anyhow::anyhow!("replica node {node_id} is not configured"))?;
        let response: OperationResponse = self
            .client
            .post(format!(
                "{}/raft/groups/{group_key}/purge",
                node.addr.trim_end_matches('/')
            ))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        response
            .error
            .map_or(Ok(()), |error| Err(anyhow::anyhow!(error)))
    }

    pub async fn purge_replica_local(&self, group_id: crate::GlobalGroupId) -> anyhow::Result<()> {
        let group_key = self.partition_group_key(group_id)?;
        self.purge_replica_key_local(group_key).await
    }

    pub async fn purge_replica_key_local(&self, group_key: GroupKey) -> anyhow::Result<()> {
        if !matches!(group_key, GroupKey::Partition(_)) {
            anyhow::bail!("control-plane group cannot be purged while the process is running");
        }
        if let Some(group) = self.groups.write().await.remove(&group_key) {
            group.raft().shutdown().await?;
        }
        let component = group_key.storage_component();
        let source = self.directory.join("groups").join(&component);
        let retired = self.directory.join("retired");
        crate::store::blocking_io::run(move || {
            if source.exists() {
                std::fs::remove_dir_all(source)?;
            }
            if retired.is_dir() {
                let prefix = format!("group-{component}-");
                for entry in std::fs::read_dir(retired)? {
                    let entry = entry?;
                    if entry.file_name().to_string_lossy().starts_with(&prefix) {
                        std::fs::remove_dir_all(entry.path())?;
                    }
                }
            }
            Ok(())
        })
        .await?;
        Ok(())
    }

    pub(super) async fn wait_replica_caught_up(
        &self,
        partition: &PartitionDescriptor,
        node_id: NodeId,
        expected_index: u64,
    ) -> anyhow::Result<()> {
        let node = self.node(node_id).expect("replica is configured");
        for _ in 0..600 {
            if let Ok(response) = self
                .client
                .get(format!(
                    "{}/raft/groups/{}/health",
                    node.addr.trim_end_matches('/'),
                    partition.group_key()
                ))
                .send()
                .await
            {
                if response.status().is_success() {
                    let health: serde_json::Value = response.json().await?;
                    if health["last_applied_index"].as_u64().unwrap_or_default() >= expected_index {
                        return Ok(());
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        anyhow::bail!(
            "replica node {node_id} did not catch up group {}",
            partition.global_id()
        )
    }
}
