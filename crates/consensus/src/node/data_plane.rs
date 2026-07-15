use super::*;
use crate::{RoutedResponse, INTERNAL_WRITE_FRAME_BYTES, INTERNAL_WRITE_RESPONSE_BYTES};
use bytes::Bytes;
use rustqueue_queue::Delivery;

const QUORUM_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

impl ConsensusNode {
    pub async fn write(&self, command: QueueCommand) -> anyhow::Result<QueueResponse> {
        let _guard = self.leadership_gate.read().await;
        self.write_unlocked(command).await
    }

    async fn write_unlocked(&self, command: QueueCommand) -> anyhow::Result<QueueResponse> {
        let mut leader = self.current_leader()?;
        let envelope = crate::CommandEnvelope::new(command);
        for _ in 0..2 {
            if leader == self.node_id {
                return self.submit_write(envelope.command).await;
            }
            let node = self
                .node(leader)
                .ok_or_else(|| anyhow::anyhow!("leader {leader} is not in configured nodes"))?;
            let _timer = self.latency.forward.timer();
            let forwarded: RoutedResponse<QueueResponse> = crate::post_binary_limited(
                &self.client,
                format!(
                    "{}/raft/groups/{}/write",
                    node.addr.trim_end_matches('/'),
                    self.group_key
                ),
                &envelope,
                INTERNAL_WRITE_FRAME_BYTES,
                INTERNAL_WRITE_RESPONSE_BYTES,
            )
            .await?;
            match forwarded {
                RoutedResponse::Success { value, .. } => return Ok(value),
                RoutedResponse::NotLeader(redirect) => {
                    leader = redirect
                        .leader_id
                        .ok_or_else(|| anyhow::anyhow!("metadata leader is unknown"))?;
                }
                RoutedResponse::Failed { message, .. } => return Err(anyhow::anyhow!(message)),
            }
        }
        anyhow::bail!("metadata leader changed repeatedly")
    }

    async fn submit_write(&self, command: QueueCommand) -> anyhow::Result<QueueResponse> {
        self.write_batcher.submit(command).await
    }

    pub fn leader_state(&self) -> (Option<NodeId>, u64) {
        let metrics = self.raft.metrics().borrow().clone();
        (metrics.current_leader, metrics.current_term)
    }

    pub async fn write_routed_local(&self, command: QueueCommand) -> RoutedResponse<QueueResponse> {
        let _guard = self.leadership_gate.read().await;
        let (leader_id, term) = self.leader_state();
        if leader_id != Some(self.node_id) {
            return RoutedResponse::not_leader(leader_id, term);
        }
        match self.submit_write(command).await {
            Ok(response) => RoutedResponse::success(response, self.node_id, term),
            Err(error) => {
                let (leader_id, term) = self.leader_state();
                if leader_id != Some(self.node_id) {
                    RoutedResponse::not_leader(leader_id, term)
                } else {
                    RoutedResponse::failed(error.to_string(), leader_id, term)
                }
            }
        }
    }

    pub async fn fetch_routed_local(&self, request: FetchRequest) -> RoutedResponse<FetchResponse> {
        let _guard = self.leadership_gate.read().await;
        let (leader_id, term) = self.leader_state();
        if leader_id != Some(self.node_id) {
            return RoutedResponse::not_leader(leader_id, term);
        }

        let result = if let (Some(partition), Some(expired_before_ns)) =
            (request.partition, request.expired_before_ns)
        {
            self.broker
                .fetch_expired_batch_partition(
                    &request.topic,
                    &request.channel,
                    partition,
                    expired_before_ns,
                    request.max_messages as usize,
                    request.max_bytes as usize,
                )
                .await
        } else if let Some(partition) = request.partition {
            self.broker
                .fetch_batch_partition(
                    &request.topic,
                    &request.channel,
                    partition,
                    request.max_messages as usize,
                    request.max_bytes as usize,
                    std::time::Duration::from_millis(request.wait_ms as u64),
                    Some(std::time::Duration::from_millis(request.timeout_ms)),
                )
                .await
        } else {
            let mut cursor = request.partition_cursor;
            self.broker
                .fetch_batch(
                    &request.topic,
                    &request.channel,
                    &mut cursor,
                    request.max_messages as usize,
                    request.max_bytes as usize,
                    std::time::Duration::from_millis(request.wait_ms as u64),
                    Some(std::time::Duration::from_millis(request.timeout_ms)),
                )
                .await
        };

        let deliveries = match result {
            Ok(deliveries) => deliveries,
            Err(error) => {
                return RoutedResponse::failed(error.to_string(), Some(self.node_id), term);
            }
        };
        if !deliveries.is_empty() {
            if let Err(error) = self.ensure_quorum_local_unlocked().await {
                let ids: Vec<_> = deliveries.iter().map(|delivery| delivery.id).collect();
                self.broker.release(&request.topic, &request.channel, &ids);
                let (leader_id, term) = self.leader_state();
                return if leader_id != Some(self.node_id) {
                    RoutedResponse::not_leader(leader_id, term)
                } else {
                    RoutedResponse::failed(
                        format!("quorum confirmation failed: {error}"),
                        leader_id,
                        term,
                    )
                };
            }
        }
        RoutedResponse::success(
            FetchResponse {
                deliveries: deliveries.into_iter().map(RemoteDelivery::from).collect(),
                partition_cursor: request.partition_cursor,
                error: None,
            },
            self.node_id,
            term,
        )
    }
    pub async fn ready_routed_local(&self, request: FetchRequest) -> RoutedResponse<bool> {
        let _guard = self.leadership_gate.read().await;
        let (leader_id, term) = self.leader_state();
        if leader_id != Some(self.node_id) {
            return RoutedResponse::not_leader(leader_id, term);
        }
        let Some(partition) = request.partition else {
            return RoutedResponse::failed("ready probe requires a partition", leader_id, term);
        };
        match self
            .broker
            .wait_partition_ready(
                &request.topic,
                &request.channel,
                partition,
                std::time::Duration::from_millis(request.wait_ms as u64),
            )
            .await
        {
            Ok(ready) => RoutedResponse::success(ready, self.node_id, term),
            Err(error) => RoutedResponse::failed(error.to_string(), leader_id, term),
        }
    }

    pub fn touch_routed_local(&self, request: TouchRequest) -> RoutedResponse<()> {
        let (leader_id, term) = self.leader_state();
        if leader_id != Some(self.node_id) {
            return RoutedResponse::not_leader(leader_id, term);
        }
        match self.touch_local(request).error {
            None => RoutedResponse::success((), self.node_id, term),
            Some(error) => RoutedResponse::failed(error, Some(self.node_id), term),
        }
    }

    pub fn release_routed_local(&self, request: ReleaseRequest) -> RoutedResponse<()> {
        let (leader_id, term) = self.leader_state();
        if leader_id != Some(self.node_id) {
            return RoutedResponse::not_leader(leader_id, term);
        }
        match self.release_local(request).error {
            None => RoutedResponse::success((), self.node_id, term),
            Some(error) => RoutedResponse::failed(error, Some(self.node_id), term),
        }
    }

    pub async fn quorum_routed_local(&self) -> RoutedResponse<()> {
        let _guard = self.leadership_gate.read().await;
        let (leader_id, term) = self.leader_state();
        if leader_id != Some(self.node_id) {
            return RoutedResponse::not_leader(leader_id, term);
        }
        match self.ensure_quorum_local_unlocked().await {
            Ok(()) => RoutedResponse::success((), self.node_id, term),
            Err(error) => {
                let (leader_id, term) = self.leader_state();
                RoutedResponse::failed(error.to_string(), leader_id, term)
            }
        }
    }

    pub fn touch_local(&self, request: TouchRequest) -> OperationResponse {
        if self.current_leader().ok() != Some(self.node_id) {
            return OperationResponse {
                error: Some("node is not the current leader".into()),
            };
        }
        OperationResponse {
            error: self
                .broker
                .touch(
                    &request.topic,
                    &request.channel,
                    request.message_id,
                    Some(std::time::Duration::from_millis(request.timeout_ms)),
                )
                .err()
                .map(|error| error.to_string()),
        }
    }

    pub fn release_local(&self, request: ReleaseRequest) -> OperationResponse {
        if self.current_leader().ok() != Some(self.node_id) {
            return OperationResponse {
                error: Some("node is not the current leader".into()),
            };
        }
        self.broker
            .release(&request.topic, &request.channel, &request.message_ids);
        OperationResponse { error: None }
    }

    pub async fn ensure_quorum(&self) -> anyhow::Result<()> {
        let _guard = self.leadership_gate.read().await;
        let mut leader = self.current_leader()?;
        for _ in 0..2 {
            if leader == self.node_id {
                return self.ensure_quorum_local_unlocked().await;
            }
            let node = self
                .node(leader)
                .ok_or_else(|| anyhow::anyhow!("leader {leader} is not in configured nodes"))?;
            let response: RoutedResponse<()> = crate::post_binary_limited(
                &self.client,
                format!(
                    "{}/raft/groups/{}/quorum",
                    node.addr.trim_end_matches('/'),
                    self.group_key
                ),
                &(),
                crate::INTERNAL_SMALL_FRAME_BYTES,
                crate::INTERNAL_SMALL_FRAME_BYTES,
            )
            .await?;
            match response {
                RoutedResponse::Success { .. } => return Ok(()),
                RoutedResponse::NotLeader(redirect) => {
                    leader = redirect
                        .leader_id
                        .ok_or_else(|| anyhow::anyhow!("group leader is unknown"))?;
                }
                RoutedResponse::Failed { message, .. } => return Err(anyhow::anyhow!(message)),
            }
        }
        anyhow::bail!("group leader changed repeatedly")
    }

    pub async fn ensure_quorum_local(&self) -> anyhow::Result<()> {
        let _guard = self.leadership_gate.read().await;
        self.ensure_quorum_local_unlocked().await
    }

    pub(super) async fn ensure_quorum_local_unlocked(&self) -> anyhow::Result<()> {
        let (leader, term) = self.leader_state();
        if leader != Some(self.node_id) {
            anyhow::bail!("node is not the current leader");
        }
        let raft = self.raft.clone();
        self.read_barrier
            .ensure(term, move || async move {
                tokio::time::timeout(QUORUM_TIMEOUT, raft.ensure_linearizable())
                    .await
                    .map_err(|_| "quorum confirmation timed out".to_owned())?
                    .map(|_| ())
                    .map_err(|error| error.to_string())
            })
            .await
            .map_err(anyhow::Error::msg)?;
        let (leader, current_term) = self.leader_state();
        if leader != Some(self.node_id) || current_term != term {
            anyhow::bail!("leadership changed during quorum confirmation");
        }
        Ok(())
    }
}

impl From<Delivery> for RemoteDelivery {
    fn from(delivery: Delivery) -> Self {
        Self {
            id: delivery.id,
            timestamp_ns: delivery.timestamp_ns,
            attempts: delivery.attempts,
            body: Bytes::from_owner(delivery.body),
        }
    }
}
