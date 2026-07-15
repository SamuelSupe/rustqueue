use super::CatalogShardId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CatalogSplitPhase {
    Plan,
    Create,
    SnapshotCopy,
    CatchUp,
    RootEpochSwitch,
    Redirect,
    RetireRange,
    Completed,
    NeedsOperator,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CatalogSplit {
    pub operation_id: u64,
    pub source_shard: CatalogShardId,
    pub target_shard: CatalogShardId,
    pub split_hash: u64,
    pub source_epoch: u64,
    pub target_voters: BTreeSet<u64>,
    pub phase: CatalogSplitPhase,
    pub copied_revision: u64,
    pub updated_at_ms: i64,
    pub error: Option<String>,
}

impl CatalogSplit {
    pub fn advance(
        &mut self,
        expected: CatalogSplitPhase,
        next: CatalogSplitPhase,
        copied_revision: u64,
        now_ms: i64,
    ) -> Result<(), String> {
        if self.phase != expected {
            return Err("Catalog split phase changed; refresh and retry".into());
        }
        if !valid_transition(expected, next) {
            return Err("invalid Catalog split transition".into());
        }
        self.phase = next;
        self.copied_revision = self.copied_revision.max(copied_revision);
        self.updated_at_ms = now_ms;
        self.error = None;
        Ok(())
    }

    pub fn needs_operator(&mut self, error: String, now_ms: i64) {
        self.phase = CatalogSplitPhase::NeedsOperator;
        self.error = Some(error);
        self.updated_at_ms = now_ms;
    }
}

fn valid_transition(from: CatalogSplitPhase, to: CatalogSplitPhase) -> bool {
    use CatalogSplitPhase::*;
    matches!(
        (from, to),
        (Plan, Create)
            | (Create, SnapshotCopy)
            | (SnapshotCopy, CatchUp)
            | (CatchUp, RootEpochSwitch)
            | (RootEpochSwitch, Redirect)
            | (Redirect, RetireRange)
            | (RetireRange, Completed)
            | (NeedsOperator, Plan)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_cannot_skip_the_root_epoch_switch() {
        let mut split = CatalogSplit {
            operation_id: 1,
            source_shard: 1,
            target_shard: 2,
            split_hash: 100,
            source_epoch: 9,
            target_voters: BTreeSet::from([1, 2, 3]),
            phase: CatalogSplitPhase::CatchUp,
            copied_revision: 20,
            updated_at_ms: 0,
            error: None,
        };
        assert!(split
            .advance(
                CatalogSplitPhase::CatchUp,
                CatalogSplitPhase::Redirect,
                21,
                1,
            )
            .is_err());
        split
            .advance(
                CatalogSplitPhase::CatchUp,
                CatalogSplitPhase::RootEpochSwitch,
                21,
                1,
            )
            .unwrap();
    }
}
