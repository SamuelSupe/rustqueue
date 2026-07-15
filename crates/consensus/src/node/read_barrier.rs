use std::future::Future;
use std::sync::Arc;

#[derive(Clone, Debug)]
struct Completion {
    generation: u64,
    term: u64,
    result: Result<(), String>,
}

#[derive(Default)]
struct BarrierState {
    generation: u64,
    in_flight_term: Option<u64>,
}

pub(super) struct ReadBarrier {
    state: tokio::sync::Mutex<BarrierState>,
    completed: tokio::sync::watch::Sender<Completion>,
}

impl ReadBarrier {
    pub(super) fn new() -> Arc<Self> {
        let (completed, _) = tokio::sync::watch::channel(Completion {
            generation: 0,
            term: 0,
            result: Ok(()),
        });
        Arc::new(Self {
            state: tokio::sync::Mutex::new(BarrierState::default()),
            completed,
        })
    }

    pub(super) async fn ensure<F, Fut>(
        self: &Arc<Self>,
        term: u64,
        operation: F,
    ) -> Result<(), String>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), String>> + Send + 'static,
    {
        let mut operation = Some(operation);
        loop {
            let (mut receiver, generation, owner) = {
                let mut state = self.state.lock().await;
                match state.in_flight_term {
                    Some(active_term) if active_term != term => {
                        (self.completed.subscribe(), state.generation, false)
                    }
                    Some(_) => (self.completed.subscribe(), state.generation, false),
                    None => {
                        state.in_flight_term = Some(term);
                        (self.completed.subscribe(), state.generation, true)
                    }
                }
            };

            if owner {
                let barrier = Arc::clone(self);
                let future = operation
                    .take()
                    .expect("read barrier operation can only have one owner")(
                );
                tokio::spawn(async move {
                    let result = future.await;
                    let mut state = barrier.state.lock().await;
                    state.generation = state.generation.wrapping_add(1);
                    state.in_flight_term = None;
                    let completion = Completion {
                        generation: state.generation,
                        term,
                        result,
                    };
                    drop(state);
                    barrier.completed.send_replace(completion);
                });
            }

            loop {
                let completion = receiver.borrow().clone();
                if completion.generation != generation {
                    if completion.term == term {
                        return completion.result;
                    }
                    break;
                }
                receiver
                    .changed()
                    .await
                    .map_err(|_| "read barrier worker stopped".to_owned())?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn concurrent_callers_share_one_operation_per_term() {
        let barrier = ReadBarrier::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..32 {
            let barrier = Arc::clone(&barrier);
            let calls = Arc::clone(&calls);
            tasks.push(tokio::spawn(async move {
                barrier
                    .ensure(7, move || async move {
                        calls.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                        Ok(())
                    })
                    .await
            }));
        }
        for task in tasks {
            task.await.unwrap().unwrap();
        }
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }
}
