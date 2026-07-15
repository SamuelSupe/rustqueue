use super::*;

impl ClusterRuntime {
    pub async fn group(&self, group_key: GroupKey) -> Option<Arc<ConsensusNode>> {
        self.groups
            .read()
            .await
            .get(&group_key)
            .filter(|group| !group.is_isolated())
            .cloned()
    }

    pub fn partition_group_key(&self, group_id: crate::GlobalGroupId) -> anyhow::Result<GroupKey> {
        self.metadata
            .partition(group_id)
            .map(|(_, partition)| partition.group_key())
            .ok_or_else(|| anyhow::anyhow!("partition group {group_id} not found"))
    }

    pub async fn partition_group(
        &self,
        group_id: crate::GlobalGroupId,
    ) -> Option<Arc<ConsensusNode>> {
        let key = self.partition_group_key(group_id).ok()?;
        self.group(key).await
    }

    pub fn metadata_group(&self) -> Arc<ConsensusNode> {
        Arc::clone(&self.metadata_group)
    }

    pub fn raft(&self) -> &crate::Raft {
        self.metadata_group.raft()
    }

    pub async fn local_group_ids(&self) -> Vec<GroupKey> {
        self.groups
            .read()
            .await
            .iter()
            .filter(|(_, group)| !group.is_isolated())
            .map(|(group_key, _)| *group_key)
            .collect()
    }

    pub async fn initialize_metadata(
        &self,
        members: BTreeMap<NodeId, BasicNode>,
    ) -> anyhow::Result<()> {
        self.metadata_group().initialize(members).await?;
        Ok(())
    }

    pub async fn add_metadata_learner(&self, node_id: NodeId) -> anyhow::Result<()> {
        self.metadata_group().add_learner(node_id).await
    }

    pub async fn add_learner(&self, node_id: NodeId) -> anyhow::Result<()> {
        self.add_metadata_learner(node_id).await
    }

    pub async fn add_learner_to(
        &self,
        group_id: crate::GlobalGroupId,
        node_id: NodeId,
    ) -> anyhow::Result<()> {
        self.partition_group(group_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("group {group_id} is not hosted locally"))?
            .add_learner(node_id)
            .await
    }

    pub async fn change_membership(
        &self,
        voters: BTreeSet<NodeId>,
        retain_removed_as_learners: bool,
    ) -> anyhow::Result<()> {
        self.metadata_group()
            .change_membership(voters, retain_removed_as_learners)
            .await
    }

    pub async fn change_group_membership(
        &self,
        group_id: crate::GlobalGroupId,
        voters: BTreeSet<NodeId>,
        retain_removed_as_learners: bool,
    ) -> anyhow::Result<()> {
        self.partition_group(group_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("group {group_id} is not hosted locally"))?
            .change_membership(voters, retain_removed_as_learners)
            .await
    }

    pub async fn transfer_leadership(&self, target: NodeId) -> anyhow::Result<()> {
        self.metadata_group().transfer_leadership(target).await
    }

    pub async fn transfer_group_leadership(
        &self,
        group_key: GroupKey,
        target: NodeId,
    ) -> anyhow::Result<()> {
        if group_key == self.metadata_group().group_key() {
            return self.metadata_group().transfer_leadership(target).await;
        }
        let group_id = group_key
            .partition_id()
            .ok_or_else(|| anyhow::anyhow!("unsupported leadership target {group_key}"))?;
        let (_, partition) = self
            .metadata
            .partition(group_id)
            .ok_or_else(|| anyhow::anyhow!("partition group {group_id} not found"))?;
        let leader = self.partition_leader(&partition).await?;
        if leader == self.node_id {
            return self
                .transfer_group_leadership_local(group_key, target)
                .await;
        }
        let node = self
            .node(leader)
            .ok_or_else(|| anyhow::anyhow!("group leader is not configured"))?;
        let response: OperationResponse = self
            .client
            .post(format!(
                "{}/raft/groups/{}/transfer/{target}",
                node.addr.trim_end_matches('/'),
                partition.group_key(),
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

    pub async fn transfer_group_leadership_local(
        &self,
        group_key: GroupKey,
        target: NodeId,
    ) -> anyhow::Result<()> {
        self.group(group_key)
            .await
            .ok_or_else(|| anyhow::anyhow!("group {group_key} is not hosted locally"))?
            .transfer_leadership(target)
            .await
    }

    pub async fn build_snapshot(&self) -> anyhow::Result<()> {
        let groups: Vec<_> = self.groups.read().await.values().cloned().collect();
        for group in groups {
            group.build_snapshot().await?;
        }
        Ok(())
    }

    pub async fn build_group_snapshot(&self, group_key: GroupKey) -> anyhow::Result<()> {
        self.group(group_key)
            .await
            .ok_or_else(|| anyhow::anyhow!("group {group_key} is not hosted locally"))?
            .build_snapshot()
            .await
    }
}
