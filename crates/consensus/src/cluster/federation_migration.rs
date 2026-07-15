use super::*;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum FederationMigrationAction {
    Describe {
        topic: String,
        partition: crate::GlobalGroupId,
    },
    Upsert {
        template: crate::TopicDescriptor,
        partition: PartitionDescriptor,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FederationMigrationForward {
    pub action: FederationMigrationAction,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct FederationMigrationResponse {
    pub topic: Option<crate::TopicDescriptor>,
    pub partition: Option<PartitionDescriptor>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MigrationReplicaStatus {
    pub node_id: NodeId,
    pub group: GroupKey,
    pub current_leader: Option<NodeId>,
    pub term: u64,
    pub last_log_index: Option<u64>,
    pub last_applied_index: Option<u64>,
    pub voters: BTreeSet<NodeId>,
    pub members: BTreeSet<NodeId>,
}

pub type MigrationReplicaStatusResponse = Result<MigrationReplicaStatus, FederationForwardError>;

impl ClusterRuntime {
    pub async fn forwarded_migration_local(
        &self,
        forward: FederationMigrationForward,
    ) -> Result<FederationMigrationResponse, FederationForwardError> {
        match forward.action {
            FederationMigrationAction::Describe { topic, partition } => {
                self.metadata_group()
                    .ensure_quorum()
                    .await
                    .map_err(|error| FederationForwardError::Unavailable(error.to_string()))?;
                let descriptor = self.metadata.topic(&topic).ok_or_else(|| {
                    FederationForwardError::StaleRoute("topic is absent from Home Cell".into())
                })?;
                let partition = descriptor
                    .partitions
                    .iter()
                    .find(|candidate| candidate.global_id() == partition)
                    .cloned()
                    .ok_or_else(|| {
                        FederationForwardError::StaleRoute(
                            "partition is absent from Home Cell".into(),
                        )
                    })?;
                Ok(FederationMigrationResponse {
                    topic: Some(descriptor),
                    partition: Some(partition),
                })
            }
            FederationMigrationAction::Upsert {
                template,
                partition,
            } => {
                let response = self
                    .metadata_group()
                    .write(QueueCommand::UpsertFederatedPartition {
                        template: template.clone(),
                        partition: partition.clone(),
                    })
                    .await
                    .map_err(|error| FederationForwardError::Unavailable(error.to_string()))?;
                ensure_response(&response)
                    .map_err(|error| FederationForwardError::Invalid(error.to_string()))?;
                if partition.lifecycle != crate::PartitionLifecycle::Retired {
                    for node_id in &partition.replicas {
                        self.ensure_replica_host(&template.name, &partition, *node_id)
                            .await
                            .map_err(|error| {
                                FederationForwardError::Unavailable(error.to_string())
                            })?;
                    }
                }
                Ok(FederationMigrationResponse {
                    topic: self.metadata.topic(&template.name),
                    partition: Some(partition),
                })
            }
        }
    }

    pub async fn migration_replica_status_local(
        &self,
        group: GroupKey,
    ) -> Result<MigrationReplicaStatus, FederationForwardError> {
        let raft = self.group(group).await.ok_or_else(|| {
            FederationForwardError::Unavailable(format!("group {group} is not hosted here"))
        })?;
        let metrics = raft.raft().metrics().borrow().clone();
        Ok(MigrationReplicaStatus {
            node_id: self.node_id,
            group,
            current_leader: metrics.current_leader,
            term: metrics.current_term,
            last_log_index: metrics.last_log_index,
            last_applied_index: metrics.last_applied.map(|log| log.index),
            voters: metrics.membership_config.voter_ids().collect(),
            members: metrics
                .membership_config
                .nodes()
                .map(|(node, _)| *node)
                .collect(),
        })
    }

    pub(super) async fn migration_cell(
        &self,
        cell: crate::CellId,
        action: FederationMigrationAction,
    ) -> Result<FederationMigrationResponse, FederationForwardError> {
        let request = FederationMigrationForward { action };
        if cell == self.metadata.snapshot().cell_id {
            return self.forwarded_migration_local(request).await;
        }
        self.post_home(
            cell,
            "migration",
            &request,
            INTERNAL_CATALOG_FRAME_BYTES,
            INTERNAL_CATALOG_FRAME_BYTES,
        )
        .await
    }

    pub(super) async fn migration_replica_status(
        &self,
        node_id: NodeId,
        group: GroupKey,
    ) -> anyhow::Result<MigrationReplicaStatus> {
        if node_id == self.node_id {
            return self
                .migration_replica_status_local(group)
                .await
                .map_err(anyhow::Error::msg);
        }
        let node = self
            .control
            .as_ref()
            .and_then(|control| control.nodes.get(&node_id))
            .cloned()
            .or_else(|| self.node(node_id))
            .ok_or_else(|| anyhow::anyhow!("migration node {node_id} is unknown"))?;
        let response: MigrationReplicaStatusResponse = crate::post_binary_limited(
            &self.client,
            format!(
                "{}/federation/migration/groups/{group}/status",
                node.addr.trim_end_matches('/')
            ),
            &(),
            INTERNAL_SMALL_FRAME_BYTES,
            INTERNAL_SMALL_FRAME_BYTES,
        )
        .await?;
        response.map_err(anyhow::Error::msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_status_rpc_keeps_the_result_envelope() {
        let group = GroupKey::partition(crate::CellId(2), 7).unwrap();
        let response: MigrationReplicaStatusResponse = Ok(MigrationReplicaStatus {
            node_id: 4,
            group,
            current_leader: Some(1),
            term: 3,
            last_log_index: Some(19),
            last_applied_index: Some(19),
            voters: BTreeSet::from([1, 2, 3]),
            members: BTreeSet::from([1, 2, 3, 4]),
        });

        let frame = crate::encode_frame(&response).unwrap();
        let decoded: MigrationReplicaStatusResponse = crate::decode_frame(&frame).unwrap();
        let status = decoded.unwrap();
        assert_eq!(status.node_id, 4);
        assert_eq!(status.group, group);
        assert_eq!(status.last_applied_index, Some(19));
    }
}
