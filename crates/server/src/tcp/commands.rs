use super::*;
use rustqueue_consensus::QueueResponse;

#[allow(clippy::too_many_arguments)]
pub(super) async fn process_command(
    parsed: ParsedCommand,
    peer: SocketAddr,
    config: &Config,
    broker: &Broker,
    metrics: &Metrics,
    authenticator: Option<&Authenticator>,
    consensus: Option<&ClusterRuntime>,
    ack_pipeline: Option<&AckPipeline>,
    operation_ids: &AtomicU64,
    ephemeral_consumers: &EphemeralConsumers,
    state: &mut SessionState,
    writer: &mut ClientWriter,
) -> anyhow::Result<bool> {
    match parsed.command {
        Command::Identify => {
            write_error(writer, "E_INVALID", "IDENTIFY may only be sent once").await?;
            return Ok(false);
        }
        Command::Auth => {
            if state.auth.is_some() || state.subscription.is_some() || state.closing {
                write_error(writer, "E_INVALID", "AUTH may only be sent once").await?;
                return Ok(false);
            }
            let Some(authenticator) = authenticator else {
                write_error(writer, "E_AUTH_DISABLED", "AUTH disabled").await?;
                return Ok(false);
            };
            let secret = parsed.body.as_deref().unwrap_or_default();
            if secret.is_empty() {
                write_error(writer, "E_BAD_BODY", "AUTH invalid body size 0").await?;
                return Ok(false);
            }
            match authenticator
                .authenticate(
                    &peer.ip().to_string(),
                    state.encrypted,
                    &state.tls_common_name,
                    secret,
                )
                .await
            {
                Ok(session) => {
                    let response = json!({
                        "identity": session.identity,
                        "identity_url": session.identity_url,
                        "permission_count": session.permission_count(),
                    });
                    state.auth = Some(session);
                    state.auth_secret = Some(secret.to_vec());
                    write_frame(writer, FrameType::Response, &serde_json::to_vec(&response)?)
                        .await?;
                }
                Err(error) => {
                    metrics.auth_failures.fetch_add(1, Ordering::Relaxed);
                    let (code, detail) = match error {
                        AuthError::Unauthorized => {
                            ("E_UNAUTHORIZED", "AUTH no authorizations found")
                        }
                        _ => ("E_AUTH_FAILED", "AUTH failed"),
                    };
                    write_error(writer, code, detail).await?;
                    return Ok(false);
                }
            }
        }
        Command::Subscribe { topic, channel } => {
            if state.subscription.is_some() {
                write_error(writer, "E_INVALID", "client is already subscribed").await?;
                return Ok(false);
            }
            if state.heartbeat.is_none() {
                write_error(writer, "E_INVALID", "cannot SUB with heartbeats disabled").await?;
                return Ok(false);
            }
            if !authorize_command(
                authenticator,
                peer,
                state,
                writer,
                "SUB",
                &topic,
                Some(&channel),
            )
            .await?
            {
                return Ok(false);
            }
            let mut ephemeral_lease = None;
            let create_result = if let Some(consensus) = consensus {
                let result = if channel.ends_with("#ephemeral") {
                    let lease_id = operation_ids.fetch_add(1, Ordering::Relaxed);
                    let result = consensus
                        .create_ephemeral_channel(
                            &topic,
                            &channel,
                            lease_id,
                            now_ms().saturating_add(
                                EPHEMERAL_LEASE.as_millis().min(i64::MAX as u128) as i64,
                            ),
                        )
                        .await;
                    if result.is_ok() {
                        ephemeral_lease = Some(lease_id);
                    }
                    result
                } else if consensus.channel_is_active(&topic, &channel) {
                    Ok(QueueResponse::default())
                } else {
                    consensus
                        .write(QueueCommand::CreateChannel {
                            topic: topic.clone(),
                            channel: channel.clone(),
                        })
                        .await
                };
                result
                    .map_err(|error| BrokerError::InvalidRecord(error.to_string()))
                    .and_then(|response| {
                        response
                            .error
                            .map_or(Ok(()), |error| Err(BrokerError::InvalidRecord(error)))
                    })
            } else {
                let result = broker.create_channel(&topic, &channel);
                if result.is_ok() && channel.ends_with("#ephemeral") {
                    *ephemeral_consumers
                        .lock()
                        .entry((topic.clone(), channel.clone()))
                        .or_default() += 1;
                }
                result
            };
            match create_result {
                Ok(()) => {
                    let sequence = operation_ids.fetch_add(1, Ordering::Relaxed);
                    let partition_count =
                        consensus.map_or(1, |cluster| cluster.active_partition_count(&topic));
                    state.subscription = Some(Subscription {
                        topic,
                        channel,
                        partition_cursor: super::cursor::partition_cursor_seed(
                            sequence,
                            partition_count,
                        ),
                        ephemeral_lease,
                    });
                    write_frame(writer, FrameType::Response, OK).await?;
                }
                Err(error) => {
                    write_broker_error(writer, "E_SUB_FAILED", error).await?;
                    return Ok(false);
                }
            }
        }
        Command::Publish { topic } => {
            if !authorize_command(authenticator, peer, state, writer, "PUB", &topic, None).await? {
                return Ok(false);
            }
            if !publish_tcp(
                broker,
                metrics,
                writer,
                "E_PUB_FAILED",
                topic,
                vec![parsed.body.unwrap_or_default()],
                Duration::ZERO,
                consensus,
                operation_ids,
            )
            .await?
            {
                return Ok(false);
            }
        }
        Command::MultiPublish { topic } => {
            if !authorize_command(authenticator, peer, state, writer, "MPUB", &topic, None).await? {
                return Ok(false);
            }
            let messages = match parse_mpub_bytes(
                parsed.body.unwrap_or_default(),
                config.queue.max_message_bytes,
            ) {
                Ok(messages) => messages,
                Err(detail) => {
                    write_error(writer, detail.code(), &detail.to_string()).await?;
                    return Ok(false);
                }
            };
            if !publish_tcp(
                broker,
                metrics,
                writer,
                "E_MPUB_FAILED",
                topic,
                messages,
                Duration::ZERO,
                consensus,
                operation_ids,
            )
            .await?
            {
                return Ok(false);
            }
        }
        Command::DeferredPublish { topic, delay_ms } => {
            if delay_ms > config.queue.max_defer_ms {
                write_error(writer, "E_INVALID", "defer exceeds configured maximum").await?;
                return Ok(false);
            }
            if !authorize_command(authenticator, peer, state, writer, "DPUB", &topic, None).await? {
                return Ok(false);
            }
            if !publish_tcp(
                broker,
                metrics,
                writer,
                "E_DPUB_FAILED",
                topic,
                vec![parsed.body.unwrap_or_default()],
                Duration::from_millis(delay_ms),
                consensus,
                operation_ids,
            )
            .await?
            {
                return Ok(false);
            }
        }
        Command::Ready(count) => {
            if state.closing {
                return Ok(true);
            }
            if state.subscription.is_none() || count > config.limits.max_rdy_count {
                write_error(writer, "E_INVALID", "RDY count is invalid").await?;
                return Ok(false);
            } else {
                state.rdy = count;
            }
        }
        Command::Finish(id) => {
            let Some(subscription) = &state.subscription else {
                write_error(writer, "E_INVALID", "client is not subscribed").await?;
                return Ok(false);
            };
            if !state.in_flight.contains_key(&id) {
                write_error(writer, "E_FIN_FAILED", "message is not in flight").await?;
                return Ok(true);
            }
            if state.pending_acks.contains(&id) {
                // A leadership change can redeliver the same message on this
                // connection before its first durable FIN completes. FIN is
                // idempotent while that acknowledgement is pending.
                return Ok(true);
            }
            let queued = ack_pipeline.is_some();
            let finish_result = if let Some(pipeline) = ack_pipeline {
                pipeline
                    .enqueue(AckRequest {
                        id,
                        kind: AckKind::Finish,
                        command: QueueCommand::Finish {
                            topic: subscription.topic.clone(),
                            channel: subscription.channel.clone(),
                            message_id: id,
                        },
                    })
                    .await
            } else {
                finish_message(
                    broker,
                    consensus,
                    &subscription.topic,
                    &subscription.channel,
                    id,
                )
                .await
            };
            match finish_result {
                Ok(()) if queued => {
                    state.pending_acks.insert(id);
                }
                Ok(()) => {
                    state.in_flight.remove(&id);
                    metrics.finished_messages.fetch_add(1, Ordering::Relaxed);
                }
                Err(error) => write_broker_error(writer, "E_FIN_FAILED", error).await?,
            }
        }
        Command::Requeue { id, delay_ms } => {
            let delay_ms = delay_ms.clamp(0, config.queue.max_defer_ms as i64) as u64;
            let Some(subscription) = &state.subscription else {
                write_error(writer, "E_INVALID", "client is not subscribed").await?;
                return Ok(false);
            };
            if !state.in_flight.contains_key(&id) {
                write_error(writer, "E_REQ_FAILED", "message is not in flight").await?;
                return Ok(true);
            }
            if state.pending_acks.contains(&id) {
                // Keep duplicate REQ idempotent while the durable operation is
                // already queued. The completion path still reports failures.
                return Ok(true);
            }
            let queued = ack_pipeline.is_some();
            let available_at_ms = now_ms().saturating_add(delay_ms.min(i64::MAX as u64) as i64);
            let requeue_result = if let Some(pipeline) = ack_pipeline {
                pipeline
                    .enqueue(AckRequest {
                        id,
                        kind: AckKind::Requeue,
                        command: QueueCommand::Requeue {
                            topic: subscription.topic.clone(),
                            channel: subscription.channel.clone(),
                            message_id: id,
                            available_at_ms,
                        },
                    })
                    .await
            } else if let Some(consensus) = consensus {
                consensus
                    .write(QueueCommand::Requeue {
                        topic: subscription.topic.clone(),
                        channel: subscription.channel.clone(),
                        message_id: id,
                        available_at_ms,
                    })
                    .await
                    .map_err(|error| BrokerError::InvalidRecord(error.to_string()))
                    .and_then(response_result)
            } else {
                broker.requeue(
                    &subscription.topic,
                    &subscription.channel,
                    id,
                    Duration::from_millis(delay_ms),
                )
            };
            match requeue_result {
                Ok(()) if queued => {
                    state.pending_acks.insert(id);
                }
                Ok(()) => {
                    state.in_flight.remove(&id);
                    metrics.requeued_messages.fetch_add(1, Ordering::Relaxed);
                }
                Err(error) => write_broker_error(writer, "E_REQ_FAILED", error).await?,
            }
        }
        Command::Touch(id) => {
            let Some(subscription) = &state.subscription else {
                write_error(writer, "E_INVALID", "client is not subscribed").await?;
                return Ok(false);
            };
            if !state.in_flight.contains_key(&id) {
                write_error(writer, "E_TOUCH_FAILED", "message is not in flight").await?;
                return Ok(true);
            }
            let touch_result = if let Some(consensus) = consensus {
                consensus
                    .touch(TouchRequest {
                        topic: subscription.topic.clone(),
                        channel: subscription.channel.clone(),
                        message_id: id,
                        timeout_ms: state.message_timeout.as_millis().min(u64::MAX as u128) as u64,
                    })
                    .await
                    .map_err(|error| BrokerError::InvalidRecord(error.to_string()))
            } else {
                broker.touch(
                    &subscription.topic,
                    &subscription.channel,
                    id,
                    Some(state.message_timeout),
                )
            };
            match touch_result {
                Ok(()) => {
                    state
                        .in_flight
                        .insert(id, Instant::now() + state.message_timeout);
                }
                Err(error) => write_broker_error(writer, "E_TOUCH_FAILED", error).await?,
            }
        }
        Command::Close => {
            if state.subscription.is_none() || state.closing {
                write_error(writer, "E_INVALID", "cannot CLS in current state").await?;
                return Ok(false);
            }
            state.closing = true;
            state.rdy = 0;
            write_frame(writer, FrameType::Response, CLOSE_WAIT).await?;
        }
        Command::Noop => {}
    }
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
async fn publish_tcp(
    broker: &Broker,
    metrics: &Metrics,
    writer: &mut ClientWriter,
    error_code: &str,
    topic: String,
    messages: Vec<Bytes>,
    delay: Duration,
    consensus: Option<&ClusterRuntime>,
    operation_ids: &AtomicU64,
) -> anyhow::Result<bool> {
    let message_count = messages.len() as u64;
    let byte_count = messages.iter().map(Bytes::len).sum::<usize>() as u64;
    let publish_result =
        publish_messages(broker, consensus, operation_ids, &topic, messages, delay).await;
    match publish_result {
        Ok(_) => {
            metrics
                .publish_messages
                .fetch_add(message_count, Ordering::Relaxed);
            metrics
                .publish_bytes
                .fetch_add(byte_count, Ordering::Relaxed);
            write_frame(writer, FrameType::Response, OK).await?;
            Ok(true)
        }
        Err(error) => {
            metrics.storage_errors.fetch_add(1, Ordering::Relaxed);
            write_broker_error(writer, error_code, error).await?;
            Ok(false)
        }
    }
}

pub(super) async fn publish_messages(
    broker: &Broker,
    consensus: Option<&ClusterRuntime>,
    operation_ids: &AtomicU64,
    topic: &str,
    messages: Vec<Bytes>,
    delay: Duration,
) -> Result<Vec<u64>, BrokerError> {
    if let Some(consensus) = consensus {
        let operation_id = operation_ids.fetch_add(1, Ordering::Relaxed);
        consensus
            .write(QueueCommand::Publish {
                operation_id,
                topic: topic.to_owned(),
                bodies: messages,
                timestamp_ns: now_ns(),
                available_at_ms: now_ms()
                    .saturating_add(delay.as_millis().min(i64::MAX as u128) as i64),
                partition: None,
                routing_key: None,
            })
            .await
            .map_err(|error| BrokerError::InvalidRecord(error.to_string()))
            .and_then(|response| {
                response.error.map_or(Ok(response.message_ids), |error| {
                    Err(BrokerError::InvalidRecord(error))
                })
            })
    } else {
        broker.publish(topic, messages, delay, None, None)
    }
}

fn response_result(response: rustqueue_consensus::QueueResponse) -> Result<(), BrokerError> {
    response
        .error
        .map_or(Ok(()), |error| Err(BrokerError::InvalidRecord(error)))
}

pub(super) async fn finish_message(
    broker: &Broker,
    consensus: Option<&ClusterRuntime>,
    topic: &str,
    channel: &str,
    id: u64,
) -> Result<(), BrokerError> {
    if let Some(consensus) = consensus {
        consensus
            .write(QueueCommand::Finish {
                topic: topic.to_owned(),
                channel: channel.to_owned(),
                message_id: id,
            })
            .await
            .map_err(|error| BrokerError::InvalidRecord(error.to_string()))
            .and_then(response_result)
    } else {
        broker.finish(topic, channel, id)
    }
}
