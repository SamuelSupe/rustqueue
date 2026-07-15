use super::*;

type FetchFuture<'a> = Pin<Box<dyn Future<Output = Result<FetchResponse, String>> + Send + 'a>>;

struct PendingFetch<'a> {
    request: FetchRequest,
    future: FetchFuture<'a>,
}

async fn poll_pending_fetch(
    pending: &mut Option<PendingFetch<'_>>,
) -> Result<FetchResponse, String> {
    pending
        .as_mut()
        .expect("disabled fetch future is never polled")
        .future
        .as_mut()
        .await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_session(
    io: BoxIo,
    mut pending: Option<ParsedCommand>,
    peer: SocketAddr,
    config: &Config,
    broker: &Broker,
    metrics: &Metrics,
    authenticator: Option<Arc<Authenticator>>,
    consensus: Option<Arc<ClusterRuntime>>,
    operation_ids: Arc<AtomicU64>,
    ephemeral_consumers: EphemeralConsumers,
    accepting: Arc<AtomicBool>,
    publish_admission: Arc<PublishAdmission>,
    connection_budget: Arc<ConnectionBudget>,
    mut state: SessionState,
) -> anyhow::Result<()> {
    let (read_half, write_half) = tokio::io::split(io);
    let mut writer = ClientWriter::new(write_half, state.output_buffer_size);
    let mut reader = BufReader::new(read_half);
    // A command body can arrive in several TCP reads. Keeping the read future
    // in its own task prevents heartbeat/delivery select branches from
    // cancelling it after the command line or length has already been read.
    let (command_tx, mut command_rx) = tokio::sync::mpsc::channel(1);
    let read_config = config.clone();
    let read_admission = Arc::clone(&publish_admission);
    let read_connection_budget = Arc::clone(&connection_budget);
    let read_task = tokio::spawn(async move {
        loop {
            let command = read_command(
                &mut reader,
                &read_config,
                &read_admission,
                &read_connection_budget,
            )
            .await;
            let failed = command.is_err();
            if command_tx.send(command).await.is_err() || failed {
                break;
            }
        }
    });
    let _read_task = AbortOnDrop(read_task.abort_handle());
    let mut heartbeat_tick = interval(state.heartbeat.unwrap_or(Duration::from_secs(3600)));
    heartbeat_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    heartbeat_tick.tick().await;
    let mut output_buffer_tick = interval(
        state
            .output_buffer_timeout
            .unwrap_or(Duration::from_secs(3600)),
    );
    output_buffer_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    output_buffer_tick.tick().await;
    let mut lease_tick = interval(EPHEMERAL_RENEW_INTERVAL);
    lease_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    lease_tick.tick().await;
    let mut last_command = Instant::now();
    let mut ack_pipeline = consensus
        .as_ref()
        .map(|runtime| AckPipeline::start(Arc::clone(runtime)));
    let mut pending_fetch: Option<PendingFetch<'_>> = None;
    let mut abandoned_deliveries = Vec::new();

    loop {
        if !accepting.load(Ordering::Acquire) {
            state.closing = true;
            state.rdy = 0;
        }
        if state.closing && state.in_flight.is_empty() {
            break;
        }
        if let Some(command) = pending.take() {
            if state.closing && !shutdown_command_allowed(&command.command) {
                write_error(&mut writer, "E_CLOSING", "node is shutting down").await?;
                continue;
            }
            last_command = Instant::now();
            if !process_command(
                command,
                peer,
                config,
                broker,
                metrics,
                authenticator.as_deref(),
                consensus.as_deref(),
                ack_pipeline.as_ref(),
                &operation_ids,
                &ephemeral_consumers,
                &mut state,
                &mut writer,
            )
            .await?
            {
                break;
            }
            continue;
        }

        let now = Instant::now();
        let pending_acks = &state.pending_acks;
        state
            .in_flight
            .retain(|id, deadline| pending_acks.contains(id) || *deadline > now);
        let available = state.rdy.saturating_sub(state.in_flight.len() as u64);
        if pending_fetch.is_none() && !state.closing && available > 0 {
            if let Some(subscription) = state.subscription.as_ref() {
                let request = FetchRequest {
                    topic: subscription.topic.clone(),
                    channel: subscription.channel.clone(),
                    partition_cursor: subscription.partition_cursor,
                    timeout_ms: state.message_timeout.as_millis().min(u64::MAX as u128) as u64,
                    max_messages: available.min(MAX_FETCH_MESSAGES as u64) as u16,
                    max_bytes: MAX_FETCH_BYTES,
                    wait_ms: DEFAULT_FETCH_WAIT_MS,
                    partition: None,
                    expired_before_ns: None,
                };
                metrics.fetch_requests.fetch_add(1, Ordering::Relaxed);
                let future = Box::pin(fetch_deliveries(
                    broker,
                    consensus.as_deref(),
                    request.clone(),
                ));
                pending_fetch = Some(PendingFetch { request, future });
            }
        }

        tokio::select! {
            completion = async {
                ack_pipeline
                    .as_mut()
                    .expect("disabled ack pipeline is never polled")
                    .recv()
                    .await
            }, if ack_pipeline.is_some() => {
                let Some(completion) = completion else {
                    anyhow::bail!("ack pipeline stopped");
                };
                apply_ack_completion(completion, metrics, &mut state, &mut writer).await?;
            }
            command = command_rx.recv() => {
                let Some(command) = command else {
                    break;
                };
                let command = match command {
                    Ok(command) => command,
                    Err(CommandReadError::Io(_)) => break,
                    Err(CommandReadError::Protocol { code, detail }) => {
                        write_error(&mut writer, code, &detail).await?;
                        break;
                    }
                };
                if state.closing && !shutdown_command_allowed(&command.command) {
                    write_error(&mut writer, "E_CLOSING", "node is shutting down").await?;
                    continue;
                }
                last_command = Instant::now();
                if !process_command(
                    command,
                    peer,
                    config,
                    broker,
                    metrics,
                    authenticator.as_deref(),
                    consensus.as_deref(),
                    ack_pipeline.as_ref(),
                    &operation_ids,
                    &ephemeral_consumers,
                    &mut state,
                    &mut writer,
                ).await? {
                    break;
                }
            }
            delivery_result = poll_pending_fetch(&mut pending_fetch), if pending_fetch.is_some() => {
                let request = pending_fetch
                    .take()
                    .expect("completed fetch retains its request")
                    .request;
                let delivery_topic = request.topic.clone();
                let delivery_channel = request.channel.clone();
                match delivery_result {
                    Ok(response) if response.error.is_none() => {
                        if state.closing {
                            abandoned_deliveries
                                .extend(response.deliveries.into_iter().map(|delivery| delivery.id));
                            continue;
                        }
                        if let Some(subscription) = state.subscription.as_mut() {
                            subscription.partition_cursor = response.partition_cursor;
                        }
                        let batch_messages = response.deliveries.len();
                        let batch_bytes = response
                            .deliveries
                            .iter()
                            .map(|delivery| delivery.body.len())
                            .sum::<usize>();
                        metrics.fetch_batches.fetch_add(1, Ordering::Relaxed);
                        metrics
                            .fetch_messages
                            .fetch_add(batch_messages as u64, Ordering::Relaxed);
                        metrics
                            .fetch_bytes
                            .fetch_add(batch_bytes as u64, Ordering::Relaxed);
                        if response.deliveries.is_empty() {
                            metrics.fetch_empty.fetch_add(1, Ordering::Relaxed);
                            writer.flush_pending().await?;
                            continue;
                        }
                        for delivery in response.deliveries {
                            if delivery_is_outstanding(
                                &state.in_flight,
                                &state.pending_acks,
                                delivery.id,
                            ) {
                                continue;
                            }
                            if dead_letter_if_needed(
                                config,
                                broker,
                                consensus.as_deref(),
                                &operation_ids,
                                metrics,
                                &delivery_topic,
                                &delivery_channel,
                                &delivery,
                            )
                            .await
                            .map_err(|error| anyhow::anyhow!(error.to_string()))?
                            {
                                continue;
                            }
                            if !state.accept_sample() {
                                if let Some(pipeline) = ack_pipeline.as_ref() {
                                    pipeline
                                        .enqueue(AckRequest {
                                            id: delivery.id,
                                            kind: AckKind::Finish,
                                            command: QueueCommand::Finish {
                                                topic: delivery_topic.clone(),
                                                channel: delivery_channel.clone(),
                                                message_id: delivery.id,
                                            },
                                        })
                                        .await
                                        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
                                    state.pending_acks.insert(delivery.id);
                                    state.in_flight.insert(
                                        delivery.id,
                                        Instant::now() + state.message_timeout,
                                    );
                                } else {
                                    finish_message(
                                        broker,
                                        consensus.as_deref(),
                                        &delivery_topic,
                                        &delivery_channel,
                                        delivery.id,
                                    )
                                    .await
                                    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
                                }
                                continue;
                            }
                            let header = encode_message_header(
                                delivery.timestamp_ns,
                                delivery.attempts,
                                delivery.id,
                                delivery.body.len(),
                            );
                            writer.write_message_parts(&header, &delivery.body).await?;
                            state
                                .in_flight
                                .insert(delivery.id, Instant::now() + state.message_timeout);
                            metrics.delivered_messages.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Ok(response) => {
                        metrics.storage_errors.fetch_add(1, Ordering::Relaxed);
                        write_error(
                            &mut writer,
                            "E_DELIVERY_FAILED",
                            response.error.as_deref().unwrap_or("delivery failed"),
                        )
                        .await?;
                        break;
                    }
                    Err(error) => {
                        metrics.storage_errors.fetch_add(1, Ordering::Relaxed);
                        write_error(&mut writer, "E_DELIVERY_FAILED", &error).await?;
                        break;
                    }
                }
            }
            _ = heartbeat_tick.tick(), if state.heartbeat.is_some() => {
                let heartbeat = state.heartbeat.unwrap();
                if last_command.elapsed() >= heartbeat.saturating_mul(2) {
                    break;
                }
                write_frame(&mut writer, FrameType::Response, HEARTBEAT).await?;
            }
            _ = output_buffer_tick.tick(), if state.output_buffer_timeout.is_some() && writer.has_pending() => {
                writer.flush_pending().await?;
            }
            _ = lease_tick.tick(), if consensus.is_some() && state.subscription.as_ref().is_some_and(|subscription| subscription.ephemeral_lease.is_some()) => {
                let subscription = state.subscription.as_ref().expect("lease has subscription");
                let lease_id = subscription.ephemeral_lease.expect("lease was checked");
                if let Err(error) = consensus.as_ref().expect("lease requires consensus")
                    .renew_ephemeral_lease(
                        &subscription.topic,
                        &subscription.channel,
                        lease_id,
                        now_ms().saturating_add(
                            EPHEMERAL_LEASE.as_millis().min(i64::MAX as u128) as i64,
                        ),
                    )
                    .await
                {
                    tracing::warn!(%error, "ephemeral channel lease renewal failed");
                    break;
                }
            }
        }
    }
    if let Some(mut fetch) = pending_fetch.take() {
        if let Ok(Ok(response)) =
            tokio::time::timeout(Duration::from_secs(5), fetch.future.as_mut()).await
        {
            abandoned_deliveries
                .extend(response.deliveries.into_iter().map(|delivery| delivery.id));
        }
    }
    if let Some(pipeline) = ack_pipeline.take() {
        for completion in pipeline.shutdown().await {
            apply_ack_completion(completion, metrics, &mut state, &mut writer).await?;
        }
    }
    if let Some(subscription) = &state.subscription {
        let mut ids: Vec<_> = state.in_flight.into_keys().collect();
        ids.append(&mut abandoned_deliveries);
        ids.sort_unstable();
        ids.dedup();
        if !ids.is_empty() {
            if let Some(consensus) = consensus.as_deref() {
                if let Err(error) = consensus
                    .release(rustqueue_consensus::ReleaseRequest {
                        topic: subscription.topic.clone(),
                        channel: subscription.channel.clone(),
                        message_ids: ids,
                    })
                    .await
                {
                    tracing::warn!(%error, "failed to release disconnected in-flight messages");
                }
            } else {
                broker.release(&subscription.topic, &subscription.channel, &ids);
            }
        }
        if subscription.channel.ends_with("#ephemeral") {
            if let (Some(consensus), Some(lease_id)) =
                (consensus.as_deref(), subscription.ephemeral_lease)
            {
                if let Err(error) = consensus
                    .release_ephemeral_lease(&subscription.topic, &subscription.channel, lease_id)
                    .await
                {
                    tracing::warn!(%error, "failed to release ephemeral channel lease");
                }
            } else if consensus.is_none() {
                let key = (subscription.topic.clone(), subscription.channel.clone());
                let delete = {
                    let mut consumers = ephemeral_consumers.lock();
                    if let Some(count) = consumers.get_mut(&key) {
                        *count = count.saturating_sub(1);
                        if *count == 0 {
                            consumers.remove(&key);
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                };
                if delete {
                    let _ = broker.delete_channel(&subscription.topic, &subscription.channel);
                }
            }
        }
    }
    Ok(())
}

async fn apply_ack_completion(
    completion: AckCompletion,
    metrics: &Metrics,
    state: &mut SessionState,
    writer: &mut ClientWriter,
) -> anyhow::Result<()> {
    state.pending_acks.remove(&completion.id);
    if let Some(error) = completion.error {
        let code = match completion.kind {
            AckKind::Finish => "E_FIN_FAILED",
            AckKind::Requeue => "E_REQ_FAILED",
        };
        write_error(writer, code, &error).await?;
        return Ok(());
    }
    state.in_flight.remove(&completion.id);
    match completion.kind {
        AckKind::Finish => {
            metrics.finished_messages.fetch_add(1, Ordering::Relaxed);
        }
        AckKind::Requeue => {
            metrics.requeued_messages.fetch_add(1, Ordering::Relaxed);
        }
    }
    Ok(())
}

async fn fetch_deliveries(
    broker: &Broker,
    consensus: Option<&ClusterRuntime>,
    mut request: FetchRequest,
) -> Result<FetchResponse, String> {
    if let Some(consensus) = consensus {
        return consensus
            .fetch(request)
            .await
            .map_err(|error| error.to_string());
    }
    let deliveries = broker
        .fetch_batch(
            &request.topic,
            &request.channel,
            &mut request.partition_cursor,
            request.max_messages as usize,
            request.max_bytes as usize,
            Duration::from_millis(request.wait_ms as u64),
            Some(Duration::from_millis(request.timeout_ms)),
        )
        .await
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|delivery| RemoteDelivery {
            id: delivery.id,
            timestamp_ns: delivery.timestamp_ns,
            attempts: delivery.attempts,
            body: bytes::Bytes::from_owner(delivery.body),
        })
        .collect();
    Ok(FetchResponse {
        deliveries,
        partition_cursor: request.partition_cursor,
        error: None,
    })
}

struct AbortOnDrop(tokio::task::AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn shutdown_command_allowed(command: &Command) -> bool {
    matches!(
        command,
        Command::Finish(_)
            | Command::Requeue { .. }
            | Command::Touch(_)
            | Command::Noop
            | Command::Close
    )
}

fn delivery_is_outstanding(
    in_flight: &HashMap<u64, Instant>,
    pending_acks: &HashSet<u64>,
    id: u64,
) -> bool {
    in_flight.contains_key(&id) || pending_acks.contains(&id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pending_fetch_survives_an_unrelated_ready_branch() {
        let request = FetchRequest {
            topic: "events".into(),
            channel: "workers".into(),
            partition_cursor: 0,
            timeout_ms: 1_000,
            max_messages: 1,
            max_bytes: 1024,
            wait_ms: 100,
            partition: None,
            expired_before_ns: None,
        };
        let future = Box::pin(async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(FetchResponse {
                deliveries: Vec::new(),
                partition_cursor: 1,
                error: None,
            })
        });
        let mut pending = Some(PendingFetch { request, future });

        tokio::select! {
            biased;
            _ = std::future::ready(()) => {}
            _ = poll_pending_fetch(&mut pending) => panic!("fetch unexpectedly won select"),
        }

        assert!(pending.is_some());
        let response = poll_pending_fetch(&mut pending).await.unwrap();
        assert_eq!(response.partition_cursor, 1);
    }

    #[test]
    fn duplicate_delivery_is_suppressed_while_in_flight_or_ack_pending() {
        let mut in_flight = HashMap::new();
        let mut pending = HashSet::new();
        in_flight.insert(7, Instant::now());
        assert!(delivery_is_outstanding(&in_flight, &pending, 7));

        in_flight.clear();
        pending.insert(7);
        assert!(delivery_is_outstanding(&in_flight, &pending, 7));
        assert!(!delivery_is_outstanding(&in_flight, &pending, 8));
    }
}
