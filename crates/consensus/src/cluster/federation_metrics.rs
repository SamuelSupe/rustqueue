use crate::PartitionMigrationPhase;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub(super) struct FederationMetrics {
    forwarded: AtomicU64,
    stale_routes: AtomicU64,
    retries: AtomicU64,
    unavailable: AtomicU64,
    stale_cache_uses: AtomicU64,
    channel_reconciliations: AtomicU64,
    migration_advances: AtomicU64,
    migrations_needing_operator: AtomicU64,
}

impl FederationMetrics {
    pub(super) fn forwarded(&self) {
        self.forwarded.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn stale_route(&self) {
        self.stale_routes.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn retry(&self) {
        self.retries.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn unavailable(&self) {
        self.unavailable.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn stale_cache_used(&self) {
        self.stale_cache_uses.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn reconciled_channels(&self, count: usize) {
        self.channel_reconciliations
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    pub(super) fn migration_advanced(&self) {
        self.migration_advances.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn migration_needs_operator(&self) {
        self.migrations_needing_operator
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn render(&self) -> String {
        format!(
            "# TYPE rustqueue_federation_forwarded_requests_total counter\n\
             # TYPE rustqueue_federation_stale_routes_total counter\n\
             # TYPE rustqueue_federation_route_retries_total counter\n\
             # TYPE rustqueue_federation_unavailable_total counter\n\
             # TYPE rustqueue_federation_stale_cache_uses_total counter\n\
             # TYPE rustqueue_federation_channel_reconciliations_total counter\n\
             # TYPE rustqueue_federation_migration_advances_total counter\n\
             # TYPE rustqueue_federation_migrations_needing_operator_total counter\n\
             rustqueue_federation_forwarded_requests_total {}\n\
             rustqueue_federation_stale_routes_total {}\n\
             rustqueue_federation_route_retries_total {}\n\
             rustqueue_federation_unavailable_total {}\n\
             rustqueue_federation_stale_cache_uses_total {}\n\
             rustqueue_federation_channel_reconciliations_total {}\n\
             rustqueue_federation_migration_advances_total {}\n\
             rustqueue_federation_migrations_needing_operator_total {}\n",
            self.forwarded.load(Ordering::Relaxed),
            self.stale_routes.load(Ordering::Relaxed),
            self.retries.load(Ordering::Relaxed),
            self.unavailable.load(Ordering::Relaxed),
            self.stale_cache_uses.load(Ordering::Relaxed),
            self.channel_reconciliations.load(Ordering::Relaxed),
            self.migration_advances.load(Ordering::Relaxed),
            self.migrations_needing_operator.load(Ordering::Relaxed),
        )
    }
}

pub(super) fn migration_phase(phase: PartitionMigrationPhase) -> &'static str {
    use PartitionMigrationPhase::*;
    match phase {
        Planned => "planned",
        PrepareTarget => "prepare_target",
        SnapshotCopy => "snapshot_copy",
        CatchUp => "catch_up",
        SourceFence => "source_fence",
        FinalCatchUp => "final_catch_up",
        Cutover => "cutover",
        DrainSource => "drain_source",
        Completed => "completed",
        NeedsOperator => "needs_operator",
        RemoveSourceLearners => "remove_source_learners",
    }
}
