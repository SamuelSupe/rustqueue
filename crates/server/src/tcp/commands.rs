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
            let create_result = broker.create_channel(&topic, &channel).await;
            if create_result.is_ok() && channel.ends_with("#ephemeral") {
                *ephemeral_consumers
                    .lock()
                    .entry((topic.clone(), channel.clone()))
                    .or_default() += 1;
            }
            match create_result {
                Ok(()) => {
                    state.subscription = Some(Subscription { topic, channel });
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
            let finish_result =
                finish_message(broker, &subscription.topic, &subscription.channel, id).await;
            match finish_result {
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
            let requeue_result = broker
                .requeue(
                    &subscription.topic,
                    &subscription.channel,
                    id,
                    Duration::from_millis(delay_ms),
                )
                .await;
            match requeue_result {
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
            let touch_result = broker.touch(
                &subscription.topic,
                &subscription.channel,
                id,
                Some(state.message_timeout),
            );
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
) -> anyhow::Result<bool> {
    let message_count = messages.len() as u64;
    let byte_count = messages.iter().map(Bytes::len).sum::<usize>() as u64;
    let publish_result = publish_messages(broker, &topic, messages, delay).await;
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
    topic: &str,
    messages: Vec<Bytes>,
    delay: Duration,
) -> Result<Vec<u64>, BrokerError> {
    broker.publish(topic, messages, delay).await
}

pub(super) async fn finish_message(
    broker: &Broker,
    topic: &str,
    channel: &str,
    id: u64,
) -> Result<(), BrokerError> {
    broker.finish(topic, channel, id).await
}
