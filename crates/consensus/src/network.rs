use crate::network_metrics::{network_metrics, RpcKind};
use crate::wire;
use crate::{
    GroupKey, NodeId, TypeConfig, INTERNAL_APPEND_FRAME_BYTES, INTERNAL_BINARY_CONTENT_TYPE,
    INTERNAL_SMALL_FRAME_BYTES, INTERNAL_SNAPSHOT_FRAME_BYTES,
};
use openraft::error::{
    InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use reqwest::header::{ACCEPT, CONTENT_TYPE};
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::Serialize;

#[derive(Clone)]
pub struct Network {
    client: Client,
    snapshot_client: Client,
    group_key: GroupKey,
}

pub struct Connection {
    network: Network,
    target: NodeId,
    node: BasicNode,
}

type RpcError<E = openraft::error::Infallible> = RPCError<NodeId, BasicNode, RaftError<NodeId, E>>;

#[derive(Serialize)]
struct AppendEntriesSlice<'a> {
    vote: &'a openraft::Vote<NodeId>,
    prev_log_id: &'a Option<openraft::LogId<NodeId>>,
    entries: &'a [openraft::Entry<TypeConfig>],
    leader_commit: &'a Option<openraft::LogId<NodeId>>,
}

impl Network {
    pub fn new(client: Client) -> Self {
        Self::for_group(client, GroupKey::cell_metadata(crate::CellId::BOOTSTRAP))
    }

    pub fn for_group(client: Client, group_key: GroupKey) -> Self {
        Self {
            snapshot_client: client.clone(),
            client,
            group_key,
        }
    }

    pub fn for_group_with_snapshot(
        client: Client,
        snapshot_client: Client,
        group_key: GroupKey,
    ) -> Self {
        Self {
            client,
            snapshot_client,
            group_key,
        }
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    async fn send<Req, Resp, Err>(
        &self,
        target: NodeId,
        node: &BasicNode,
        path: &str,
        request: &Req,
    ) -> Result<Resp, RPCError<NodeId, BasicNode, Err>>
    where
        Req: Serialize + ?Sized,
        Resp: DeserializeOwned,
        Err: std::error::Error + DeserializeOwned,
    {
        let (client, request_limit, kind) = if path == "snapshot" {
            (
                &self.snapshot_client,
                INTERNAL_SNAPSHOT_FRAME_BYTES,
                RpcKind::Snapshot,
            )
        } else if path == "append" {
            (&self.client, INTERNAL_APPEND_FRAME_BYTES, RpcKind::Append)
        } else {
            (&self.client, INTERNAL_SMALL_FRAME_BYTES, RpcKind::Vote)
        };
        let url = format!(
            "{}/raft/groups/{}/{}",
            node.addr.trim_end_matches('/'),
            self.group_key,
            path
        );
        let body = wire::encode_frame_with_limit(request, request_limit)
            .map_err(|error| RPCError::Network(NetworkError::new(&error)))?;
        network_metrics().record_request(kind, body.len());
        let response = client
            .post(&url)
            .header(CONTENT_TYPE, INTERNAL_BINARY_CONTENT_TYPE)
            .header(ACCEPT, INTERNAL_BINARY_CONTENT_TYPE)
            .body(body)
            .send()
            .await
            .map_err(|error| {
                if error.is_connect() || error.is_timeout() {
                    RPCError::Unreachable(Unreachable::new(&error))
                } else {
                    RPCError::Network(NetworkError::new(&error))
                }
            })?
            .error_for_status()
            .map_err(|error| RPCError::Network(NetworkError::new(&error)))?;
        let bytes = response
            .bytes()
            .await
            .map_err(|error| RPCError::Network(NetworkError::new(&error)))?;
        network_metrics().record_response(kind, bytes.len());
        let result: Result<Resp, Err> =
            wire::decode_frame_with_limit(&bytes, INTERNAL_SMALL_FRAME_BYTES)
                .map_err(|error| RPCError::Network(NetworkError::new(&error)))?;
        result.map_err(|error| RPCError::RemoteError(RemoteError::new(target, error)))
    }
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = Connection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        Connection {
            network: self.clone(),
            target,
            node: node.clone(),
        }
    }
}

impl RaftNetwork<TypeConfig> for Connection {
    async fn append_entries(
        &mut self,
        request: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RpcError> {
        let prefix = append_prefix_len(&request, INTERNAL_APPEND_FRAME_BYTES);
        if prefix == 0 || prefix == request.entries.len() {
            return self
                .network
                .send(self.target, &self.node, "append", &request)
                .await;
        }
        let matched = request.entries[prefix - 1].log_id;
        let partial = AppendEntriesSlice {
            vote: &request.vote,
            prev_log_id: &request.prev_log_id,
            entries: &request.entries[..prefix],
            leader_commit: &request.leader_commit,
        };
        let response: AppendEntriesResponse<NodeId> = self
            .network
            .send(self.target, &self.node, "append", &partial)
            .await?;
        Ok(match response {
            AppendEntriesResponse::Success => AppendEntriesResponse::PartialSuccess(Some(matched)),
            response => response,
        })
    }

    async fn install_snapshot(
        &mut self,
        request: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, RpcError<InstallSnapshotError>> {
        self.network
            .send(self.target, &self.node, "snapshot", &request)
            .await
    }

    async fn vote(
        &mut self,
        request: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RpcError> {
        self.network
            .send(self.target, &self.node, "vote", &request)
            .await
    }
}

fn append_prefix_len(request: &AppendEntriesRequest<TypeConfig>, max_bytes: usize) -> usize {
    if request.entries.is_empty() {
        return 0;
    }
    let fits = |entries: &[openraft::Entry<TypeConfig>]| {
        let partial = AppendEntriesSlice {
            vote: &request.vote,
            prev_log_id: &request.prev_log_id,
            entries,
            leader_commit: &request.leader_commit,
        };
        wire::encoded_frame_len(&partial).is_ok_and(|bytes| bytes <= max_bytes)
    };
    if fits(&request.entries) {
        return request.entries.len();
    }
    if !fits(&request.entries[..1]) {
        return 0;
    }
    let mut low = 1;
    let mut high = request.entries.len() - 1;
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if fits(&request.entries[..middle]) {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    low
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::QueueCommand;
    use openraft::{CommittedLeaderId, EntryPayload, LogId, Vote};

    #[test]
    fn append_catch_up_is_batched_by_bytes_and_stays_wire_compatible() {
        let entries = (1..=64)
            .map(|index| openraft::Entry {
                log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
                payload: EntryPayload::Normal(crate::CommandEnvelope::new(QueueCommand::Publish {
                    operation_id: index,
                    topic: "events".into(),
                    bodies: vec![bytes::Bytes::from(vec![index as u8; 1024])],
                    timestamp_ns: 0,
                    available_at_ms: 0,
                    partition: Some(0),
                    routing_key: None,
                })),
            })
            .collect();
        let request = AppendEntriesRequest {
            vote: Vote::new_committed(1, 1),
            prev_log_id: None,
            entries,
            leader_commit: None,
        };
        let prefix = append_prefix_len(&request, 32 * 1024);
        assert!(prefix > 1 && prefix < request.entries.len());

        let partial = AppendEntriesSlice {
            vote: &request.vote,
            prev_log_id: &request.prev_log_id,
            entries: &request.entries[..prefix],
            leader_commit: &request.leader_commit,
        };
        let frame = wire::encode_frame_with_limit(&partial, 32 * 1024).unwrap();
        let decoded: AppendEntriesRequest<TypeConfig> =
            wire::decode_frame_with_limit(&frame, 32 * 1024).unwrap();
        assert_eq!(decoded.entries.len(), prefix);
        assert_eq!(
            decoded.entries.last().unwrap().log_id,
            request.entries[prefix - 1].log_id
        );
    }
}
