use super::*;
use crate::network_metrics::{network_metrics, RpcKind};
use crate::INTERNAL_BINARY_CONTENT_TYPE;
use bytes::Bytes;
use reqwest::header::{ACCEPT, CONTENT_TYPE};
use std::collections::VecDeque;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CachedLeader {
    leader_id: NodeId,
    term: u64,
    epoch: u64,
}

#[derive(Default)]
pub(super) struct LeaderRoutes {
    entries: parking_lot::RwLock<HashMap<crate::GlobalGroupId, CachedLeader>>,
}

impl LeaderRoutes {
    fn observe(&self, partition: &PartitionDescriptor, leader_id: NodeId, term: u64) {
        if !partition.replicas.contains(&leader_id) {
            return;
        }
        let epoch = topology_epoch(partition);
        let mut entries = self.entries.write();
        if entries
            .get(&partition.global_id())
            .is_some_and(|current| current.epoch == epoch && current.term > term)
        {
            return;
        }
        entries.insert(
            partition.global_id(),
            CachedLeader {
                leader_id,
                term,
                epoch,
            },
        );
    }

    pub(super) fn prefers_local(&self, partition: &PartitionDescriptor, node_id: NodeId) -> bool {
        let epoch = topology_epoch(partition);
        self.entries
            .read()
            .get(&partition.global_id())
            .filter(|entry| entry.epoch == epoch)
            .is_none_or(|entry| entry.leader_id == node_id)
    }

    fn candidates(&self, partition: &PartitionDescriptor) -> VecDeque<NodeId> {
        let epoch = topology_epoch(partition);
        let cached = self
            .entries
            .read()
            .get(&partition.global_id())
            .copied()
            .filter(|entry| entry.epoch == epoch)
            .map(|entry| entry.leader_id);
        let mut candidates = VecDeque::new();
        for candidate in cached
            .into_iter()
            .chain(partition.leader_hint)
            .chain(partition.replicas.iter().copied())
        {
            if partition.replicas.contains(&candidate) && !candidates.contains(&candidate) {
                candidates.push_back(candidate);
            }
        }
        candidates
    }
}

impl ClusterRuntime {
    pub(super) fn accept_routed<T>(
        &self,
        partition: &PartitionDescriptor,
        response: RoutedResponse<T>,
    ) -> anyhow::Result<Option<T>> {
        match response {
            RoutedResponse::Success {
                value,
                leader_id,
                term,
            } => {
                self.leader_routes.observe(partition, leader_id, term);
                Ok(Some(value))
            }
            RoutedResponse::NotLeader(redirect) => {
                network_metrics().record_redirect();
                if let Some(leader_id) = redirect.leader_id {
                    self.leader_routes
                        .observe(partition, leader_id, redirect.term);
                }
                Ok(None)
            }
            RoutedResponse::Failed { message, .. } => Err(anyhow::anyhow!(message)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn post_to_leader<Req, Resp>(
        &self,
        partition: &PartitionDescriptor,
        operation: &str,
        request: &Req,
        kind: RpcKind,
        request_limit: usize,
        response_limit: usize,
    ) -> anyhow::Result<Resp>
    where
        Req: Serialize + ?Sized,
        Resp: serde::de::DeserializeOwned,
    {
        let _timer = self.forward_latency.timer();
        let frame = Bytes::from(crate::encode_frame_with_limit(request, request_limit)?);
        let mut candidates = self.leader_routes.candidates(partition);
        let mut attempted = BTreeSet::new();
        let mut last_error = None;

        while let Some(node_id) = candidates.pop_front() {
            if !attempted.insert(node_id) {
                continue;
            }
            if attempted.len() > 1 {
                network_metrics().record_retry();
            }
            let Some(node) = self.node(node_id) else {
                continue;
            };
            network_metrics().record_request(kind, frame.len());
            let response = self
                .client
                .post(format!(
                    "{}/raft/groups/{}/{}",
                    node.addr.trim_end_matches('/'),
                    partition.group_key(),
                    operation
                ))
                .header(CONTENT_TYPE, INTERNAL_BINARY_CONTENT_TYPE)
                .header(ACCEPT, INTERNAL_BINARY_CONTENT_TYPE)
                .body(frame.clone())
                .send()
                .await;
            let response = match response {
                Ok(response) => match response.error_for_status() {
                    Ok(response) => response,
                    Err(error) => {
                        last_error = Some(error.into());
                        continue;
                    }
                },
                Err(error) => {
                    last_error = Some(error.into());
                    continue;
                }
            };
            if response
                .content_length()
                .is_some_and(|length| length > response_limit as u64)
            {
                last_error = Some(anyhow::anyhow!(
                    "internal RPC response exceeds endpoint limit"
                ));
                continue;
            }
            let bytes = response.bytes().await?;
            network_metrics().record_response(kind, bytes.len());
            let routed: RoutedResponse<Resp> =
                crate::decode_frame_with_limit(&bytes, response_limit)?;
            match routed {
                RoutedResponse::Success {
                    value,
                    leader_id,
                    term,
                } => {
                    self.leader_routes.observe(partition, leader_id, term);
                    return Ok(value);
                }
                RoutedResponse::NotLeader(redirect) => {
                    network_metrics().record_redirect();
                    if let Some(leader_id) = redirect.leader_id {
                        self.leader_routes
                            .observe(partition, leader_id, redirect.term);
                        if partition.replicas.contains(&leader_id)
                            && !attempted.contains(&leader_id)
                        {
                            candidates.push_front(leader_id);
                        }
                    }
                }
                RoutedResponse::Failed { message, .. } => {
                    return Err(anyhow::anyhow!(message));
                }
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("partition has no reachable leader")))
    }
}

fn topology_epoch(partition: &PartitionDescriptor) -> u64 {
    let mut bytes = Vec::with_capacity(16 + partition.replicas.len() * 8);
    bytes.extend_from_slice(&partition.origin_cell.0.to_le_bytes());
    bytes.extend_from_slice(&partition.group_id.to_le_bytes());
    for node_id in &partition.replicas {
        bytes.extend_from_slice(&node_id.to_le_bytes());
    }
    crc32c::crc32c(&bytes) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partition(replicas: BTreeSet<NodeId>) -> PartitionDescriptor {
        PartitionDescriptor {
            group_id: 7,
            origin_cell: crate::CellId::BOOTSTRAP,
            number: 0,
            slot: 1,
            replication_factor: replicas.len() as u8,
            replicas,
            leader_hint: None,
            lifecycle: crate::PartitionLifecycle::Active,
            operation_id: None,
            home_cell: crate::CellId::BOOTSTRAP,
            wire_incarnation: 1,
        }
    }

    #[test]
    fn cache_prefers_newest_term_and_invalidates_on_membership_epoch() {
        let routes = LeaderRoutes::default();
        let first = partition(BTreeSet::from([1, 2, 3]));
        routes.observe(&first, 2, 4);
        routes.observe(&first, 3, 3);
        assert_eq!(routes.candidates(&first).front(), Some(&2));

        let changed = partition(BTreeSet::from([1, 3, 4]));
        assert_ne!(routes.candidates(&changed).front(), Some(&2));
        routes.observe(&changed, 4, 5);
        assert_eq!(routes.candidates(&changed).front(), Some(&4));
    }
}
