use super::*;

impl StateMachineStore {
    pub(crate) async fn prune_snapshot_generations(&self, keep: usize) -> io::Result<usize> {
        let generations = self.generations.clone();
        let latency = Arc::clone(&self.latency);
        blocking_io::run(move || {
            let _timer = latency.gc.timer();
            generations.prune_old(keep)
        })
        .await
    }
}
