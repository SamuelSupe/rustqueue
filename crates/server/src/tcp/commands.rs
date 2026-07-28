use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn process_command(
    parsed: ParsedCommand,
    peer: SocketAddr,
    config: &Config,
    broker: &Broker,
    metrics: &Metrics,
    authenticator: Option<&Authenticator>,
    ephemeral_consumers: &EphemeralConsumers,
    subscriptions: &SubscriptionRegistry,
    channel_ops: &ChannelOpSender,
    state: &mut SessionState,
    writer: &mut ClientWriter,
) -> anyhow::Result<bool> {
    let ParsedCommand {
        command,
        body,
        publish_reservation,
    } = parsed;
    match command {
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
            let secret = body.unwrap_or_default();
            if secret.is_empty() {
                write_error(writer, "E_BAD_BODY", "AUTH invalid body size 0").await?;
                return Ok(false);
            }
            match authenticator
                .authenticate(
                    &peer.ip().to_string(),
                    state.encrypted,
                    &state.tls_common_name,
                    &secret,
                )
                .await
            {
                Ok(session) => {
                    let response = json!({
                        "identity": session.identity(),
                        "identity_url": session.identity_url(),
                        "permission_count": session.permission_count(),
                    });
                    state.auth = Some(session);
                    state.auth_secret = Some(secret);
                    state._auth_reservation = publish_reservation;
                    state.client_identity.authed = true;
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
            let lease =
                match subscriptions.register(&topic, &channel, state.client_identity.clone()) {
                    Ok(lease) => lease,
                    Err(_) => {
                        write_error(writer, "E_SUB_FAILED", "channel deletion is in progress")
                            .await?;
                        return Ok(false);
                    }
                };
            let create_result = if channel.ends_with("#ephemeral") {
                ephemeral_consumers.register(broker, &topic, &channel).await
            } else {
                broker.create_channel(&topic, &channel).await
            };
            match create_result {
                Ok(()) => {
                    state.subscription = Some(Subscription {
                        topic,
                        channel,
                        lease,
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
                vec![body.unwrap_or_default()],
                Duration::ZERO,
                publish_reservation,
                config.limits.disconnect_on_retriable_publish_error,
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
            let messages =
                match parse_mpub_bytes(body.unwrap_or_default(), config.queue.max_message_bytes) {
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
                publish_reservation,
                config.limits.disconnect_on_retriable_publish_error,
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
                vec![body.unwrap_or_default()],
                Duration::from_millis(delay_ms),
                publish_reservation,
                config.limits.disconnect_on_retriable_publish_error,
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
                state.update_subscription_flow();
            }
        }
        Command::Finish(id) => {
            let Some(subscription) = &state.subscription else {
                write_error(writer, "E_INVALID", "client is not subscribed").await?;
                return Ok(false);
            };
            let Some(delivery) = state.delivery_for_operation(id) else {
                write_error(writer, "E_FIN_FAILED", "message is not in flight").await?;
                return Ok(true);
            };
            let deadline = match renew_delivery_lease(
                broker,
                &subscription.topic,
                &subscription.channel,
                id,
                delivery.token,
                state.message_timeout,
            ) {
                Ok(deadline) => deadline,
                Err(error) => {
                    write_broker_error(writer, "E_FIN_FAILED", error).await?;
                    return Ok(true);
                }
            };
            let result = channel_ops.finish(
                subscription.topic.clone(),
                subscription.channel.clone(),
                id,
                delivery.token,
            );
            if let Err(error) = result {
                write_broker_error(writer, "E_FIN_FAILED", error).await?;
            } else {
                state.in_flight.insert(
                    id,
                    InFlightDelivery {
                        deadline,
                        ..delivery
                    },
                );
                state.pending_channel_ops.insert(id);
            }
        }
        Command::Requeue { id, delay_ms } => {
            let delay_ms = delay_ms.clamp(0, config.queue.max_defer_ms as i64) as u64;
            let Some(subscription) = &state.subscription else {
                write_error(writer, "E_INVALID", "client is not subscribed").await?;
                return Ok(false);
            };
            let Some(delivery) = state.delivery_for_operation(id) else {
                write_error(writer, "E_REQ_FAILED", "message is not in flight").await?;
                return Ok(true);
            };
            let deadline = match renew_delivery_lease(
                broker,
                &subscription.topic,
                &subscription.channel,
                id,
                delivery.token,
                state.message_timeout,
            ) {
                Ok(deadline) => deadline,
                Err(error) => {
                    write_broker_error(writer, "E_REQ_FAILED", error).await?;
                    return Ok(true);
                }
            };
            let result = channel_ops.requeue(
                subscription.topic.clone(),
                subscription.channel.clone(),
                id,
                delivery.token,
                Duration::from_millis(delay_ms),
            );
            if let Err(error) = result {
                write_broker_error(writer, "E_REQ_FAILED", error).await?;
            } else {
                state.in_flight.insert(
                    id,
                    InFlightDelivery {
                        deadline,
                        ..delivery
                    },
                );
                state.pending_channel_ops.insert(id);
            }
        }
        Command::Touch(id) => {
            let Some(subscription) = &state.subscription else {
                write_error(writer, "E_INVALID", "client is not subscribed").await?;
                return Ok(false);
            };
            let Some(delivery) = state.delivery_for_operation(id) else {
                write_error(writer, "E_TOUCH_FAILED", "message is not in flight").await?;
                return Ok(true);
            };
            let touch_result = renew_delivery_lease(
                broker,
                &subscription.topic,
                &subscription.channel,
                id,
                delivery.token,
                state.message_timeout,
            );
            match touch_result {
                Ok(deadline) => {
                    state.in_flight.insert(
                        id,
                        InFlightDelivery {
                            deadline,
                            ..delivery
                        },
                    );
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
            state.update_subscription_flow();
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
    reservation: Option<PublishReservation>,
    disconnect_on_retriable_error: bool,
) -> anyhow::Result<bool> {
    let message_count = messages.len() as u64;
    let byte_count = messages.iter().map(Bytes::len).sum::<usize>() as u64;
    let publish_result = publish_messages(broker, &topic, messages, delay, reservation).await;
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
            if broker_storage_error(&error) {
                metrics.storage_errors.fetch_add(1, Ordering::Relaxed);
            }
            let retryable = precommit_retryable_publish_error(&error);
            if disconnect_on_retriable_error && retryable {
                return Ok(false);
            }
            write_broker_error(
                writer,
                if retryable { "E_PUB_RETRY" } else { error_code },
                error,
            )
            .await?;
            Ok(false)
        }
    }
}

fn precommit_retryable_publish_error(error: &BrokerError) -> bool {
    matches!(
        error,
        BrokerError::TopicRetiring
            | BrokerError::ManagementUnavailable
            | BrokerError::TopicLimit
            | BrokerError::PublishWorkerLimit
            | BrokerError::SequenceExhausted
    )
}

pub(super) async fn publish_messages(
    broker: &Broker,
    topic: &str,
    messages: Vec<Bytes>,
    delay: Duration,
    reservation: Option<PublishReservation>,
) -> Result<Vec<u64>, BrokerError> {
    match reservation {
        Some(reservation) => {
            broker
                .publish_guarded(topic, messages, delay, reservation)
                .await
        }
        None => broker.publish(topic, messages, delay).await,
    }
}

#[cfg(test)]
mod tests {
    use super::{broker_storage_error, precommit_retryable_publish_error};
    use rustqueue_queue::BrokerError;

    #[test]
    fn only_guaranteed_precommit_failures_are_retryable() {
        assert!(precommit_retryable_publish_error(
            &BrokerError::TopicRetiring
        ));
        assert!(precommit_retryable_publish_error(
            &BrokerError::PublishWorkerLimit
        ));
        assert!(precommit_retryable_publish_error(
            &BrokerError::SequenceExhausted
        ));
        assert!(!precommit_retryable_publish_error(
            &BrokerError::StorageUnavailable
        ));
        assert!(!precommit_retryable_publish_error(
            &BrokerError::InvalidRecord("corrupt local state".into())
        ));
        assert!(!precommit_retryable_publish_error(
            &BrokerError::MessageTooLarge
        ));
        assert!(!precommit_retryable_publish_error(
            &BrokerError::TopicTombstoned
        ));
    }

    #[test]
    fn only_storage_failures_increment_the_storage_error_metric() {
        assert!(broker_storage_error(&BrokerError::StorageUnavailable));
        assert!(broker_storage_error(&BrokerError::InvalidRecord(
            "corrupt local state".into()
        )));
        assert!(!broker_storage_error(&BrokerError::TopicLimit));
        assert!(!broker_storage_error(&BrokerError::TopicRetiring));
    }
}
