use super::*;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

// Channel operations stay queued per session, but only this many hold active
// broker futures while waiting for a durable group commit.
const MAX_PENDING_CHANNEL_OPS: usize = 256;

#[derive(Clone, Copy)]
pub(super) enum ChannelOpKind {
    Finish,
    SampleFinish,
    Requeue,
}

impl ChannelOpKind {
    pub(super) fn error_code(self) -> &'static str {
        match self {
            Self::Finish | Self::SampleFinish => "E_FIN_FAILED",
            Self::Requeue => "E_REQ_FAILED",
        }
    }
}

enum ChannelOp {
    Finish {
        topic: Arc<str>,
        channel: Arc<str>,
        id: u64,
        token: u64,
        sampled: bool,
    },
    Requeue {
        topic: Arc<str>,
        channel: Arc<str>,
        id: u64,
        token: u64,
        delay: Duration,
    },
}

impl ChannelOp {
    fn id(&self) -> u64 {
        match self {
            Self::Finish { id, .. } | Self::Requeue { id, .. } => *id,
        }
    }

    fn kind(&self) -> ChannelOpKind {
        match self {
            Self::Finish { sampled: true, .. } => ChannelOpKind::SampleFinish,
            Self::Finish { .. } => ChannelOpKind::Finish,
            Self::Requeue { .. } => ChannelOpKind::Requeue,
        }
    }
}

pub(super) struct ChannelOpCompletion {
    pub(super) id: u64,
    pub(super) kind: ChannelOpKind,
    pub(super) result: Result<(), BrokerError>,
}

#[derive(Clone)]
pub(super) struct ChannelOpSender {
    sender: mpsc::UnboundedSender<ChannelOp>,
}

impl ChannelOpSender {
    pub(super) fn finish(
        &self,
        topic: Arc<str>,
        channel: Arc<str>,
        id: u64,
        token: u64,
    ) -> Result<(), BrokerError> {
        self.send(ChannelOp::Finish {
            topic,
            channel,
            id,
            token,
            sampled: false,
        })
    }

    pub(super) fn finish_sampled(
        &self,
        topic: Arc<str>,
        channel: Arc<str>,
        id: u64,
        token: u64,
    ) -> Result<(), BrokerError> {
        self.send(ChannelOp::Finish {
            topic,
            channel,
            id,
            token,
            sampled: true,
        })
    }

    pub(super) fn requeue(
        &self,
        topic: Arc<str>,
        channel: Arc<str>,
        id: u64,
        token: u64,
        delay: Duration,
    ) -> Result<(), BrokerError> {
        self.send(ChannelOp::Requeue {
            topic,
            channel,
            id,
            token,
            delay,
        })
    }

    fn send(&self, operation: ChannelOp) -> Result<(), BrokerError> {
        self.sender
            .send(operation)
            .map_err(|_| BrokerError::StorageUnavailable)
    }
}

pub(super) fn start_channel_ops(
    broker: Broker,
) -> (
    ChannelOpSender,
    mpsc::UnboundedReceiver<ChannelOpCompletion>,
    JoinHandle<()>,
) {
    // The session admits at most one operation per in-flight message, and
    // in-flight messages are capped by max_rdy_count.
    let (operation_tx, operation_rx) = mpsc::unbounded_channel();
    let (completion_tx, completion_rx) = mpsc::unbounded_channel();
    let task = tokio::spawn(run_channel_ops(broker, operation_rx, completion_tx));
    (
        ChannelOpSender {
            sender: operation_tx,
        },
        completion_rx,
        task,
    )
}

async fn run_channel_ops(
    broker: Broker,
    mut operations: mpsc::UnboundedReceiver<ChannelOp>,
    completions: mpsc::UnboundedSender<ChannelOpCompletion>,
) {
    let mut pending = FuturesUnordered::new();
    let mut receiving = true;
    while receiving || !pending.is_empty() {
        tokio::select! {
            operation = operations.recv(), if receiving && pending.len() < MAX_PENDING_CHANNEL_OPS => {
                match operation {
                    Some(operation) => {
                        let broker = broker.clone();
                        pending.push(execute_channel_op(broker, operation));
                    }
                    None => receiving = false,
                }
            }
            completion = pending.next(), if !pending.is_empty() => {
                if let Some(completion) = completion {
                    let _ = completions.send(completion);
                }
            }
        }
    }
}

async fn execute_channel_op(broker: Broker, operation: ChannelOp) -> ChannelOpCompletion {
    let id = operation.id();
    let kind = operation.kind();
    let result = match operation {
        ChannelOp::Finish {
            topic,
            channel,
            id,
            token,
            ..
        } => {
            broker
                .finish_delivery_shared(&topic, channel, id, token)
                .await
        }
        ChannelOp::Requeue {
            topic,
            channel,
            id,
            token,
            delay,
        } => {
            broker
                .requeue_delivery_shared(&topic, channel, id, token, delay)
                .await
        }
    };
    ChannelOpCompletion { id, kind, result }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustqueue_queue::BrokerConfig;
    use tempfile::tempdir;

    #[tokio::test]
    async fn one_session_pipeline_forms_durable_groups() {
        let root = tempdir().unwrap();
        let broker = Broker::open(BrokerConfig {
            data_path: root.path().into(),
            ..BrokerConfig::default()
        })
        .unwrap();
        broker.create_channel("events", "workers").await.unwrap();
        broker
            .publish(
                "events",
                (0..32).map(|_| vec![b'x']).collect(),
                Duration::ZERO,
            )
            .await
            .unwrap();
        let batch = broker
            .fetch_batch_retained("events", "workers", 32, usize::MAX, Duration::ZERO, None)
            .await
            .unwrap();
        let (deliveries, mut guard) = batch.into_parts();

        let (sender, mut completions, task) = start_channel_ops(broker.clone());
        for delivery in deliveries {
            let token = guard.accept_with_token(delivery.id).unwrap();
            sender
                .finish("events".into(), "workers".into(), delivery.id, token)
                .unwrap();
        }
        drop(sender);

        let mut completed = 0;
        while let Some(completion) = completions.recv().await {
            completion.result.unwrap();
            completed += 1;
        }
        task.await.unwrap();

        let stats = broker.stats().channel_group_commit;
        assert_eq!(completed, 32);
        assert_eq!(stats.requests, 32);
        assert!(stats.commits < stats.requests);
        assert!(stats.max_batch_requests > 1);
    }
}
