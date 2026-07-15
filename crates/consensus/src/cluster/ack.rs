use super::*;
use futures::future::join_all;

#[derive(Clone, Debug)]
pub struct AckWriteResult {
    pub message_id: u64,
    pub error: Option<String>,
}

impl ClusterRuntime {
    pub async fn write_ack_batch(&self, commands: Vec<QueueCommand>) -> Vec<AckWriteResult> {
        crate::network_metrics::network_metrics().record_ack_batch(commands.len());
        if self.control_plane_enabled() {
            return self.write_federated_ack_batch(commands).await;
        }
        let mut grouped: HashMap<
            crate::GlobalGroupId,
            (PartitionDescriptor, Vec<(u64, QueueCommand)>),
        > = HashMap::new();
        let mut results = Vec::new();
        for command in commands {
            let Some((topic, message_id)) = command_message(&command) else {
                results.push(AckWriteResult {
                    message_id: 0,
                    error: Some("ack batch contains a non-ack command".into()),
                });
                continue;
            };
            match self.partition_for_message(topic, message_id) {
                Ok(partition) => grouped
                    .entry(partition.global_id())
                    .or_insert_with(|| (partition, Vec::new()))
                    .1
                    .push((message_id, command)),
                Err(error) => results.push(AckWriteResult {
                    message_id,
                    error: Some(error.to_string()),
                }),
            }
        }

        let committed = join_all(
            grouped
                .into_values()
                .map(|(partition, commands)| async move {
                    let message_ids: Vec<_> = commands.iter().map(|(id, _)| *id).collect();
                    let batch = QueueCommand::Batch {
                        commands: commands.into_iter().map(|(_, command)| command).collect(),
                    };
                    match self
                        .write_partition_kind(
                            &partition,
                            batch,
                            crate::network_metrics::RpcKind::Ack,
                        )
                        .await
                    {
                        Ok(response) => ack_results(message_ids, response),
                        Err(error) => message_ids
                            .into_iter()
                            .map(|message_id| AckWriteResult {
                                message_id,
                                error: Some(error.to_string()),
                            })
                            .collect(),
                    }
                }),
        )
        .await;
        results.extend(committed.into_iter().flatten());
        results
    }

    async fn write_federated_ack_batch(&self, commands: Vec<QueueCommand>) -> Vec<AckWriteResult> {
        let mut grouped = BTreeMap::<crate::GlobalGroupId, Vec<(u64, QueueCommand)>>::new();
        let mut results = Vec::new();
        for command in commands {
            let Some((topic, message_id)) = command_message(&command) else {
                results.push(AckWriteResult {
                    message_id: 0,
                    error: Some("ack batch contains a non-ack command".into()),
                });
                continue;
            };
            match self.catalog_message_route(topic, message_id).await {
                Ok(route) => grouped
                    .entry(route.partition.id)
                    .or_default()
                    .push((message_id, command)),
                Err(error) => results.push(AckWriteResult {
                    message_id,
                    error: Some(error.to_string()),
                }),
            }
        }
        let committed = join_all(grouped.into_values().map(|commands| async move {
            let message_ids = commands.iter().map(|(id, _)| *id).collect::<Vec<_>>();
            let Some((topic, first_id)) = commands.first().and_then(|(id, command)| {
                command_message(command).map(|(topic, _)| (topic.to_owned(), *id))
            }) else {
                return Vec::new();
            };
            let batch = QueueCommand::Batch {
                commands: commands.into_iter().map(|(_, command)| command).collect(),
            };
            match self.write_catalog_message(&topic, first_id, batch).await {
                Ok(response) => ack_results(message_ids, response),
                Err(error) => message_ids
                    .into_iter()
                    .map(|message_id| AckWriteResult {
                        message_id,
                        error: Some(error.to_string()),
                    })
                    .collect(),
            }
        }))
        .await;
        results.extend(committed.into_iter().flatten());
        results
    }
}

fn ack_results(message_ids: Vec<u64>, response: QueueResponse) -> Vec<AckWriteResult> {
    if let Some(error) = response.error {
        return message_ids
            .into_iter()
            .map(|message_id| AckWriteResult {
                message_id,
                error: Some(error.clone()),
            })
            .collect();
    }
    if response.results.len() != message_ids.len() {
        return message_ids
            .into_iter()
            .map(|message_id| AckWriteResult {
                message_id,
                error: Some("ack batch returned an incomplete result set".into()),
            })
            .collect();
    }
    message_ids
        .into_iter()
        .zip(response.results)
        .map(|(message_id, result)| AckWriteResult {
            message_id,
            error: result.error,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incomplete_batch_response_fails_every_ack() {
        let results = ack_results(vec![1, 2], QueueResponse::default());
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|result| result.error.is_some()));
    }
}
