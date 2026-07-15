use super::*;
use std::collections::HashMap as StdHashMap;

const ACK_QUEUE_DEPTH: usize = 4096;
const ACK_BATCH_MESSAGES: usize = 64;
const ACK_BATCH_DELAY: Duration = Duration::from_millis(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AckKind {
    Finish,
    Requeue,
}

pub(super) struct AckRequest {
    pub id: u64,
    pub kind: AckKind,
    pub command: QueueCommand,
}

pub(super) struct AckCompletion {
    pub id: u64,
    pub kind: AckKind,
    pub error: Option<String>,
}

pub(super) struct AckPipeline {
    sender: Option<tokio::sync::mpsc::Sender<AckRequest>>,
    completions: tokio::sync::mpsc::UnboundedReceiver<AckCompletion>,
    task: tokio::task::JoinHandle<()>,
}

impl AckPipeline {
    pub fn start(runtime: Arc<ClusterRuntime>) -> Self {
        let (sender, receiver) = tokio::sync::mpsc::channel(ACK_QUEUE_DEPTH);
        let (completion_sender, completions) = tokio::sync::mpsc::unbounded_channel();
        let task = tokio::spawn(run_ack_pipeline(runtime, receiver, completion_sender));
        Self {
            sender: Some(sender),
            completions,
            task,
        }
    }

    pub async fn enqueue(&self, request: AckRequest) -> Result<(), BrokerError> {
        self.sender
            .as_ref()
            .ok_or_else(|| BrokerError::InvalidRecord("ack pipeline is closed".into()))?
            .send(request)
            .await
            .map_err(|_| BrokerError::InvalidRecord("ack pipeline stopped".into()))
    }

    pub async fn recv(&mut self) -> Option<AckCompletion> {
        self.completions.recv().await
    }

    pub async fn shutdown(mut self) -> Vec<AckCompletion> {
        self.sender.take();
        if tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .is_err()
        {
            tracing::warn!("timed out while flushing the connection ack pipeline");
        }
        let mut completed = Vec::new();
        while let Ok(completion) = self.completions.try_recv() {
            completed.push(completion);
        }
        completed
    }
}

async fn run_ack_pipeline(
    runtime: Arc<ClusterRuntime>,
    mut receiver: tokio::sync::mpsc::Receiver<AckRequest>,
    completions: tokio::sync::mpsc::UnboundedSender<AckCompletion>,
) {
    while let Some(first) = receiver.recv().await {
        let mut batch = vec![first];
        let deadline = tokio::time::sleep(ACK_BATCH_DELAY);
        tokio::pin!(deadline);
        while batch.len() < ACK_BATCH_MESSAGES {
            tokio::select! {
                request = receiver.recv() => match request {
                    Some(request) => batch.push(request),
                    None => break,
                },
                _ = &mut deadline => break,
            }
        }

        let mut kinds: StdHashMap<_, _> = batch
            .iter()
            .map(|request| (request.id, request.kind))
            .collect();
        let results = runtime
            .write_ack_batch(batch.into_iter().map(|request| request.command).collect())
            .await;
        for result in results {
            let kind = kinds.remove(&result.message_id).unwrap_or(AckKind::Finish);
            if completions
                .send(AckCompletion {
                    id: result.message_id,
                    kind,
                    error: result.error,
                })
                .is_err()
            {
                return;
            }
        }
        for (id, kind) in kinds {
            if completions
                .send(AckCompletion {
                    id,
                    kind,
                    error: Some("ack batch omitted a result".into()),
                })
                .is_err()
            {
                return;
            }
        }
    }
}
