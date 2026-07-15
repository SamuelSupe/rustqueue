use crate::NodeId;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct LeaderRedirect {
    pub leader_id: Option<NodeId>,
    pub term: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum RoutedResponse<T> {
    Success {
        value: T,
        leader_id: NodeId,
        term: u64,
    },
    NotLeader(LeaderRedirect),
    Failed {
        message: String,
        leader_id: Option<NodeId>,
        term: u64,
    },
}

impl<T> RoutedResponse<T> {
    pub fn success(value: T, leader_id: NodeId, term: u64) -> Self {
        Self::Success {
            value,
            leader_id,
            term,
        }
    }

    pub fn not_leader(leader_id: Option<NodeId>, term: u64) -> Self {
        Self::NotLeader(LeaderRedirect { leader_id, term })
    }

    pub fn failed(message: impl Into<String>, leader_id: Option<NodeId>, term: u64) -> Self {
        Self::Failed {
            message: message.into(),
            leader_id,
            term,
        }
    }
}
