use super::*;

const TARGET_ELECTION_POLL_ATTEMPTS: usize = 10;
const TARGET_ELECTION_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

#[derive(Debug, thiserror::Error)]
pub(super) enum TargetSelectionError {
    #[error("target Cell cannot satisfy RF and failure-domain constraints: {0}")]
    Unsafe(String),
    #[error("target Cell is temporarily unavailable: {0}")]
    Unavailable(String),
}

pub(super) struct MigrationView {
    pub(super) leader: NodeId,
    pub(super) target_lag: u64,
    pub(super) voters: BTreeSet<NodeId>,
    pub(super) members: BTreeSet<NodeId>,
}

impl ClusterRuntime {
    pub(super) async fn select_migration_targets(
        &self,
        cell: crate::CellId,
        replication_factor: usize,
    ) -> Result<BTreeSet<NodeId>, TargetSelectionError> {
        let control = self.control.as_ref().ok_or_else(|| {
            TargetSelectionError::Unsafe("independent control plane is disabled".into())
        })?;
        let root = self
            .root_snapshot_fresh()
            .await
            .map_err(|error| TargetSelectionError::Unavailable(error.to_string()))?;
        let descriptor = root.cells.get(&cell).ok_or_else(|| {
            TargetSelectionError::Unsafe(format!("target Cell {cell} is unknown"))
        })?;
        let configured = descriptor
            .nodes
            .iter()
            .filter_map(|node_id| {
                let node = root.nodes.get(node_id)?;
                node.available
                    .then_some((*node_id, node.failure_domain.clone()))
            })
            .collect::<Vec<_>>();
        let domains = configured
            .iter()
            .map(|(_, domain)| domain)
            .collect::<BTreeSet<_>>();
        if configured.len() < replication_factor || domains.len() < replication_factor {
            return Err(TargetSelectionError::Unsafe(format!(
                "Cell {cell} has fewer than {replication_factor} available failure domains"
            )));
        }

        let mut eligible = Vec::new();
        for (node_id, domain) in configured {
            let Some(node) = control.nodes.get(&node_id) else {
                continue;
            };
            let response = self
                .client
                .get(format!("{}/raft/time", node.addr.trim_end_matches('/')))
                .send()
                .await;
            let Ok(response) = response else { continue };
            let Ok(response) = response.error_for_status() else {
                continue;
            };
            let Ok(status) = response.json::<serde_json::Value>().await else {
                continue;
            };
            if status["clock_healthy"].as_bool().unwrap_or(false)
                && status["gateway_ready"].as_bool().unwrap_or(false)
                && status["disk"]["eligible"].as_bool().unwrap_or(false)
            {
                eligible.push((domain, node_id));
            }
        }
        eligible.sort();
        let mut selected = BTreeSet::new();
        let mut selected_domains = BTreeSet::new();
        for (domain, node_id) in eligible {
            if selected_domains.insert(domain) {
                selected.insert(node_id);
                if selected.len() == replication_factor {
                    return Ok(selected);
                }
            }
        }
        Err(TargetSelectionError::Unavailable(format!(
            "fewer than {replication_factor} healthy storage targets are reachable in Cell {cell}"
        )))
    }

    pub(super) async fn migration_view(
        &self,
        source: &PartitionDescriptor,
        target: &PartitionDescriptor,
    ) -> Result<MigrationView, TargetSelectionError> {
        let statuses = self.migration_statuses(source, target).await;
        migration_view_from_statuses(&statuses, target)
    }

    pub(super) async fn migration_view_with_target_election(
        &self,
        source: &PartitionDescriptor,
        target: &PartitionDescriptor,
    ) -> Result<MigrationView, TargetSelectionError> {
        let statuses = self.migration_statuses(source, target).await;
        if needs_target_election(&statuses, &target.replicas) {
            return self.elect_migration_target(source, target).await;
        }
        migration_view_from_statuses(&statuses, target)
    }

    async fn elect_migration_target(
        &self,
        source: &PartitionDescriptor,
        target: &PartitionDescriptor,
    ) -> Result<MigrationView, TargetSelectionError> {
        let path = format!("/raft/groups/{}/elect", source.group_key());
        for candidate in &target.replicas {
            if self
                .post_group_operation(std::slice::from_ref(candidate), path.clone(), None)
                .await
                .is_err()
            {
                continue;
            }
            for _ in 0..TARGET_ELECTION_POLL_ATTEMPTS {
                tokio::time::sleep(TARGET_ELECTION_POLL_INTERVAL).await;
                let statuses = self.migration_statuses(source, target).await;
                if let Ok(view) = migration_view_from_statuses(&statuses, target) {
                    if target.replicas.contains(&view.leader) {
                        return Ok(view);
                    }
                }
            }
        }
        Err(TargetSelectionError::Unavailable(
            "promoted target voters did not elect a leader".into(),
        ))
    }

    pub(super) async fn add_migration_learners(
        &self,
        source: &PartitionDescriptor,
        target: &PartitionDescriptor,
    ) -> Result<(), TargetSelectionError> {
        for learner in &target.replicas {
            let view = migration_leader_view(&self.migration_statuses(source, target).await)?;
            if view.members.contains(learner) {
                continue;
            }
            let mut candidates = vec![view.leader];
            candidates.extend(source.replicas.iter().copied());
            self.post_group_operation(
                &candidates,
                format!("/raft/groups/{}/learners/{learner}", source.group_key()),
                None,
            )
            .await?;
        }
        Ok(())
    }

    pub(super) async fn move_migration_membership(
        &self,
        source: &PartitionDescriptor,
        target: &PartitionDescriptor,
        view: &MigrationView,
    ) -> Result<(), TargetSelectionError> {
        if view.voters == target.replicas {
            return Ok(());
        }
        if !target
            .replicas
            .iter()
            .all(|node| view.members.contains(node))
        {
            return Err(TargetSelectionError::Unavailable(
                "not every target replica is an installed learner".into(),
            ));
        }
        let mut candidates = vec![view.leader];
        candidates.extend(view.members.iter().copied());
        self.post_group_operation(
            &candidates,
            format!("/raft/groups/{}/membership", source.group_key()),
            Some(&crate::ChangeMembershipRequest {
                voters: target.replicas.clone(),
                retain_removed_as_learners: true,
            }),
        )
        .await?;
        self.elect_migration_target(source, target)
            .await
            .map(|_| ())
    }

    pub(super) async fn finalize_migration_membership(
        &self,
        source: &PartitionDescriptor,
        target: &PartitionDescriptor,
        view: &MigrationView,
    ) -> Result<(), TargetSelectionError> {
        if view.voters != target.replicas {
            return Err(TargetSelectionError::Unavailable(
                "target voters have not completed joint consensus".into(),
            ));
        }
        if view.members == target.replicas {
            return Ok(());
        }
        let mut candidates = vec![view.leader];
        candidates.extend(view.members.iter().copied());
        self.post_group_operation(
            &candidates,
            format!("/raft/groups/{}/membership", source.group_key()),
            Some(&crate::ChangeMembershipRequest {
                voters: target.replicas.clone(),
                retain_removed_as_learners: false,
            }),
        )
        .await
    }

    pub(super) async fn retire_migration_sources(
        &self,
        source: &PartitionDescriptor,
    ) -> Result<(), TargetSelectionError> {
        let control = self
            .control
            .as_ref()
            .ok_or_else(|| TargetSelectionError::Unavailable("control plane is disabled".into()))?;
        for node_id in &source.replicas {
            let Some(node) = control.nodes.get(node_id) else {
                continue;
            };
            let response = self
                .client
                .post(format!(
                    "{}/raft/groups/{}/retire",
                    node.addr.trim_end_matches('/'),
                    source.group_key()
                ))
                .send()
                .await
                .map_err(unavailable)?;
            let operation = response
                .error_for_status()
                .map_err(unavailable)?
                .json::<OperationResponse>()
                .await
                .map_err(unavailable)?;
            if let Some(error) = operation.error {
                return Err(TargetSelectionError::Unavailable(error));
            }
        }
        Ok(())
    }

    async fn migration_statuses(
        &self,
        source: &PartitionDescriptor,
        target: &PartitionDescriptor,
    ) -> Vec<MigrationReplicaStatus> {
        let mut statuses = Vec::new();
        for node_id in source.replicas.union(&target.replicas).copied() {
            if let Ok(status) = self
                .migration_replica_status(node_id, source.group_key())
                .await
            {
                statuses.push(status);
            }
        }
        statuses
    }

    async fn post_group_operation(
        &self,
        candidates: &[NodeId],
        path: String,
        body: Option<&crate::ChangeMembershipRequest>,
    ) -> Result<(), TargetSelectionError> {
        let control = self
            .control
            .as_ref()
            .ok_or_else(|| TargetSelectionError::Unavailable("control plane is disabled".into()))?;
        let mut attempted = BTreeSet::new();
        let mut errors = Vec::new();
        for node_id in candidates {
            if !attempted.insert(*node_id) {
                continue;
            }
            let Some(node) = control.nodes.get(node_id) else {
                continue;
            };
            let request = self
                .client
                .post(format!("{}{}", node.addr.trim_end_matches('/'), path));
            let response = match body {
                Some(body) => request.json(body).send().await,
                None => request.send().await,
            };
            let Ok(response) = response else {
                errors.push(format!("node {node_id} transport failure"));
                continue;
            };
            let Ok(response) = response.error_for_status() else {
                errors.push(format!("node {node_id} rejected operation"));
                continue;
            };
            let Ok(operation) = response.json::<OperationResponse>().await else {
                errors.push(format!("node {node_id} returned an invalid response"));
                continue;
            };
            if operation.error.is_none() {
                return Ok(());
            }
            errors.push(format!("node {node_id}: operation failed"));
        }
        Err(TargetSelectionError::Unavailable(errors.join("; ")))
    }
}

fn migration_view_from_statuses(
    statuses: &[MigrationReplicaStatus],
    target: &PartitionDescriptor,
) -> Result<MigrationView, TargetSelectionError> {
    let view = migration_leader_view(statuses)?;
    let leader_applied = statuses
        .iter()
        .find(|status| status.node_id == view.leader)
        .and_then(|status| status.last_applied_index)
        .unwrap_or_default();
    let mut target_applied = Vec::new();
    for node_id in &target.replicas {
        let applied = statuses
            .iter()
            .find(|status| status.node_id == *node_id)
            .and_then(|status| status.last_applied_index)
            .ok_or_else(|| {
                TargetSelectionError::Unavailable(format!(
                    "target learner {node_id} is unreachable"
                ))
            })?;
        target_applied.push(applied);
    }
    Ok(MigrationView {
        target_lag: leader_applied
            .saturating_sub(target_applied.into_iter().min().unwrap_or_default()),
        ..view
    })
}

fn needs_target_election(
    statuses: &[MigrationReplicaStatus],
    target_voters: &BTreeSet<NodeId>,
) -> bool {
    !statuses.is_empty()
        && statuses
            .iter()
            .all(|status| status.current_leader.is_none())
        && statuses
            .iter()
            .any(|status| status.voters == *target_voters)
}

fn migration_leader_view(
    statuses: &[MigrationReplicaStatus],
) -> Result<MigrationView, TargetSelectionError> {
    let leader = statuses
        .iter()
        .find_map(|status| status.current_leader)
        .ok_or_else(|| {
            TargetSelectionError::Unavailable("partition has no observable leader".into())
        })?;
    let leader_status = statuses
        .iter()
        .find(|status| status.node_id == leader)
        .ok_or_else(|| {
            TargetSelectionError::Unavailable("partition leader is unreachable".into())
        })?;
    Ok(MigrationView {
        leader,
        target_lag: u64::MAX,
        voters: leader_status.voters.clone(),
        members: leader_status.members.clone(),
    })
}

pub(super) fn unavailable(error: impl ToString) -> TargetSelectionError {
    TargetSelectionError::Unavailable(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status(
        node_id: NodeId,
        leader: Option<NodeId>,
        voters: BTreeSet<NodeId>,
    ) -> MigrationReplicaStatus {
        MigrationReplicaStatus {
            node_id,
            group: GroupKey::partition(crate::CellId(1), 9).unwrap(),
            current_leader: leader,
            term: 2,
            last_log_index: Some(10),
            last_applied_index: Some(10),
            voters: voters.clone(),
            members: voters,
        }
    }

    #[test]
    fn target_election_is_requested_only_after_voter_cutover() {
        let source = BTreeSet::from([1, 2, 3]);
        let target = BTreeSet::from([4, 5, 6]);
        let promoted = vec![status(4, None, target.clone())];
        assert!(needs_target_election(&promoted, &target));

        let learners = vec![status(1, None, source)];
        assert!(!needs_target_election(&learners, &target));

        let elected = vec![status(4, Some(4), target.clone())];
        assert!(!needs_target_election(&elected, &target));
    }
}
