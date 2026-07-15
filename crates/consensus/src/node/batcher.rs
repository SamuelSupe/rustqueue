use crate::latency::GroupLatencyMetrics;
use crate::{CommandEnvelope, QueueCommand, QueueResponse, Raft};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

const WRITE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
const MAX_BATCH_COMMANDS: usize = 64;
const MAX_BATCH_BODY_BYTES: usize = 8 * 1024 * 1024;
const MAX_SINGLE_COMMAND_BODY_BYTES: usize = 64 * 1024 * 1024;

pub(super) struct WriteBatcher {
    sender: mpsc::Sender<WriteRequest>,
}

struct WriteRequest {
    command: QueueCommand,
    response: oneshot::Sender<Result<QueueResponse, String>>,
}

impl WriteBatcher {
    pub(super) fn new(raft: Raft, latency: Arc<GroupLatencyMetrics>) -> Self {
        let (sender, receiver) = mpsc::channel(4096);
        tokio::spawn(run(raft, receiver, latency));
        Self { sender }
    }

    pub(super) async fn submit(&self, command: QueueCommand) -> anyhow::Result<QueueResponse> {
        let body_bytes = command_body_bytes(&command);
        if body_bytes > MAX_SINGLE_COMMAND_BODY_BYTES {
            anyhow::bail!(
                "Raft command body exceeds the {MAX_SINGLE_COMMAND_BODY_BYTES} byte single-command limit"
            );
        }
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(WriteRequest { command, response })
            .await
            .map_err(|_| anyhow::anyhow!("Raft write batcher stopped"))?;
        receiver
            .await
            .map_err(|_| anyhow::anyhow!("Raft write batcher dropped response"))?
            .map_err(anyhow::Error::msg)
    }
}

async fn run(
    raft: Raft,
    mut receiver: mpsc::Receiver<WriteRequest>,
    latency: Arc<GroupLatencyMetrics>,
) {
    let mut pending = None;
    loop {
        let first = match pending.take() {
            Some(request) => request,
            None => match receiver.recv().await {
                Some(request) => request,
                None => return,
            },
        };
        let mut body_bytes = command_body_bytes(&first.command);
        let mut requests = vec![first];
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(1);
        while body_bytes <= MAX_BATCH_BODY_BYTES && requests.len() < MAX_BATCH_COMMANDS {
            let next = match tokio::time::timeout_at(deadline, receiver.recv()).await {
                Ok(Some(request)) => request,
                _ => break,
            };
            let next_bytes = command_body_bytes(&next.command);
            if body_bytes.saturating_add(next_bytes) > MAX_BATCH_BODY_BYTES {
                pending = Some(next);
                break;
            }
            body_bytes = body_bytes.saturating_add(next_bytes);
            requests.push(next);
        }
        complete_batch(&raft, requests, &latency).await;
    }
}

async fn complete_batch(raft: &Raft, requests: Vec<WriteRequest>, latency: &GroupLatencyMetrics) {
    let expected = requests.len();
    let mut responses = Vec::with_capacity(expected);
    let commands = requests
        .into_iter()
        .map(|request| {
            responses.push(request.response);
            request.command
        })
        .collect();
    let result = {
        let _timer = latency.group_commit.timer();
        tokio::time::timeout(
            WRITE_TIMEOUT,
            raft.client_write(CommandEnvelope::new(QueueCommand::Batch { commands })),
        )
        .await
    };
    match result {
        Ok(Ok(response)) if response.data.results.len() == expected => {
            for (sender, response) in responses.into_iter().zip(response.data.results) {
                let _ = sender.send(Ok(response));
            }
        }
        Ok(Ok(response)) => complete_with_error(
            responses,
            format!(
                "Raft batch response count mismatch: expected {}, got {}",
                expected,
                response.data.results.len()
            ),
        ),
        Ok(Err(error)) => complete_with_error(responses, error.to_string()),
        Err(_) => complete_with_error(responses, "Raft write timed out waiting for quorum".into()),
    }
}

fn complete_with_error(
    responses: Vec<oneshot::Sender<Result<QueueResponse, String>>>,
    error: String,
) {
    for response in responses {
        let _ = response.send(Err(error.clone()));
    }
}

fn command_body_bytes(command: &QueueCommand) -> usize {
    match command {
        QueueCommand::Publish { bodies, .. } => bodies.iter().map(bytes::Bytes::len).sum(),
        QueueCommand::Batch { commands } => commands.iter().map(command_body_bytes).sum(),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_body_limit_counts_nested_batches() {
        let body = bytes::Bytes::from(vec![0; 1024]);
        let publish = QueueCommand::Publish {
            operation_id: 1,
            topic: "events".into(),
            bodies: vec![body.clone(), body],
            timestamp_ns: 0,
            available_at_ms: 0,
            partition: Some(0),
            routing_key: None,
        };
        let batch = QueueCommand::Batch {
            commands: vec![
                publish,
                QueueCommand::EmptyTopic {
                    topic: "events".into(),
                },
            ],
        };
        assert_eq!(command_body_bytes(&batch), 2048);
    }

    #[test]
    fn a_large_command_bypasses_the_small_group_commit_limit() {
        let command = QueueCommand::Publish {
            operation_id: 1,
            topic: "events".into(),
            bodies: vec![bytes::Bytes::from(vec![0; MAX_BATCH_BODY_BYTES + 1])],
            timestamp_ns: 0,
            available_at_ms: 0,
            partition: Some(0),
            routing_key: None,
        };
        assert_eq!(command_body_bytes(&command), MAX_BATCH_BODY_BYTES + 1);
        assert!(command_body_bytes(&command) <= MAX_SINGLE_COMMAND_BODY_BYTES);
    }
}
