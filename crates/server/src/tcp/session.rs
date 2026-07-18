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
    ephemeral_consumers: EphemeralConsumers,
    accepting: Arc<AtomicBool>,
    delivering: Arc<AtomicBool>,
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
    let mut last_command = Instant::now();
    let mut pending_fetch: Option<PendingFetch<'_>> = None;
    let mut abandoned_deliveries = Vec::new();

    let session_result: anyhow::Result<()> = async {
        loop {
            if state.closing && state.in_flight.is_empty() {
                break;
            }
            if let Some(command) = pending.take() {
                if !accepting.load(Ordering::Acquire) && publish_command(&command.command) {
                    write_error_timed(
                        &mut writer,
                        state.heartbeat,
                        "E_DRAINING",
                        "broker is draining",
                    )
                    .await?;
                    continue;
                }
                if !publish_admission.storage_ready() && publish_command(&command.command) {
                    write_error_timed(
                        &mut writer,
                        state.heartbeat,
                        "E_THROTTLED",
                        "local disk is above its publish watermark",
                    )
                    .await?;
                    continue;
                }
                if state.closing && !shutdown_command_allowed(&command.command) {
                    write_error_timed(
                        &mut writer,
                        state.heartbeat,
                        "E_CLOSING",
                        "node is shutting down",
                    )
                    .await?;
                    continue;
                }
                last_command = Instant::now();
                let progress_timeout = connection_progress_timeout(state.heartbeat);
                let keep_open = tokio::time::timeout(
                    progress_timeout,
                    process_command(
                        command,
                        peer,
                        config,
                        broker,
                        metrics,
                        authenticator.as_deref(),
                        &ephemeral_consumers,
                        &mut state,
                        &mut writer,
                    ),
                )
                .await
                .map_err(|_| anyhow::anyhow!("client command timed out"))??;
                if !keep_open {
                    break;
                }
                continue;
            }

            let now = Instant::now();
            state.in_flight.retain(|_, deadline| *deadline > now);
            let available = state.rdy.saturating_sub(state.in_flight.len() as u64);
            if pending_fetch.is_none()
                && !state.closing
                && delivering.load(Ordering::Acquire)
                && available > 0
            {
                if let Some(subscription) = state.subscription.as_ref() {
                    let request = FetchRequest {
                        topic: subscription.topic.clone(),
                        channel: subscription.channel.clone(),
                        timeout_ms: state.message_timeout.as_millis().min(u64::MAX as u128) as u64,
                        max_messages: available.min(MAX_FETCH_MESSAGES as u64) as u16,
                        max_bytes: config
                            .limits
                            .connection_delivery_inflight_bytes
                            .min(u32::MAX as usize) as u32,
                        wait_ms: DEFAULT_FETCH_WAIT_MS,
                    };
                    metrics.fetch_requests.fetch_add(1, Ordering::Relaxed);
                    let future = Box::pin(fetch_deliveries(broker, request.clone()));
                    pending_fetch = Some(PendingFetch { request, future });
                }
            }

            tokio::select! {
                command = command_rx.recv() => {
                    let Some(command) = command else {
                        break;
                    };
                    let command = match command {
                        Ok(command) => command,
                        Err(CommandReadError::Io(_)) => break,
                        Err(CommandReadError::Protocol { code, detail }) => {
                            write_error_timed(&mut writer, state.heartbeat, code, &detail).await?;
                            break;
                        }
                    };
                    if !accepting.load(Ordering::Acquire) && publish_command(&command.command) {
                        write_error_timed(&mut writer, state.heartbeat, "E_DRAINING", "broker is draining").await?;
                        continue;
                    }
                    if !publish_admission.storage_ready() && publish_command(&command.command) {
                        write_error_timed(&mut writer, state.heartbeat, "E_THROTTLED", "local disk is above its publish watermark").await?;
                        continue;
                    }
                    if state.closing && !shutdown_command_allowed(&command.command) {
                        write_error_timed(&mut writer, state.heartbeat, "E_CLOSING", "node is shutting down").await?;
                        continue;
                    }
                    last_command = Instant::now();
                    let progress_timeout = connection_progress_timeout(state.heartbeat);
                    let keep_open = tokio::time::timeout(
                        progress_timeout,
                        process_command(
                            command,
                            peer,
                            config,
                            broker,
                            metrics,
                            authenticator.as_deref(),
                            &ephemeral_consumers,
                            &mut state,
                            &mut writer,
                        ),
                    )
                    .await
                    .map_err(|_| anyhow::anyhow!("client command timed out"))??;
                    if !keep_open {
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
                        Ok(response) => {
                            if state.closing || !delivering.load(Ordering::Acquire) {
                                continue;
                            }
                            let FetchResponse {
                                deliveries,
                                mut delivery_guard,
                            } = response;
                            let batch_messages = deliveries.len();
                            let batch_bytes = deliveries
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
                            if deliveries.is_empty() {
                                metrics.fetch_empty.fetch_add(1, Ordering::Relaxed);
                                flush_timed(&mut writer, state.heartbeat).await?;
                                continue;
                            }
                            for delivery in deliveries {
                                if delivery_is_outstanding(
                                    &state.in_flight,
                                    delivery.id,
                                ) {
                                    continue;
                                }
                                if dead_letter_if_needed(
                                    config,
                                    broker,
                                    metrics,
                                    &delivery_topic,
                                    &delivery_channel,
                                    &delivery,
                                )
                                .await
                                .map_err(|error| anyhow::anyhow!(error.to_string()))?
                                {
                                    delivery_guard.accept(delivery.id);
                                    continue;
                                }
                                if !state.accept_sample() {
                                    finish_message(
                                        broker,
                                        &delivery_topic,
                                        &delivery_channel,
                                        delivery.id,
                                    )
                                    .await
                                    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
                                    delivery_guard.accept(delivery.id);
                                    continue;
                                }
                                let header = encode_message_header(
                                    delivery.timestamp_ns,
                                    delivery.attempts,
                                    delivery.id,
                                    delivery.body.len(),
                                );
                                tokio::time::timeout(
                                    delivery_write_timeout(state.heartbeat),
                                    writer.write_message_parts(&header, &delivery.body),
                                )
                                .await
                                .map_err(|_| anyhow::anyhow!("consumer delivery write timed out"))??;
                                state
                                    .in_flight
                                    .insert(delivery.id, Instant::now() + state.message_timeout);
                                delivery_guard.accept(delivery.id);
                                metrics.delivered_messages.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Err(error) => {
                            metrics.storage_errors.fetch_add(1, Ordering::Relaxed);
                            write_error_timed(&mut writer, state.heartbeat, "E_DELIVERY_FAILED", &error).await?;
                            break;
                        }
                    }
                }
                _ = heartbeat_tick.tick(), if state.heartbeat.is_some() => {
                    let heartbeat = state.heartbeat.unwrap();
                    if last_command.elapsed() >= heartbeat.saturating_mul(2) {
                        break;
                    }
                    tokio::time::timeout(
                        connection_progress_timeout(state.heartbeat),
                        write_frame(&mut writer, FrameType::Response, HEARTBEAT),
                    )
                    .await
                    .map_err(|_| anyhow::anyhow!("heartbeat write timed out"))??;
                }
                _ = output_buffer_tick.tick(), if state.output_buffer_timeout.is_some() && writer.has_pending() => {
                    flush_timed(&mut writer, state.heartbeat).await?;
                }
            }
        }
        Ok(())
    }
    .await;
    if let Some(mut fetch) = pending_fetch.take() {
        if let Ok(Ok(response)) =
            tokio::time::timeout(Duration::from_secs(5), fetch.future.as_mut()).await
        {
            abandoned_deliveries
                .extend(response.deliveries.into_iter().map(|delivery| delivery.id));
        }
    }
    if let Some(subscription) = &state.subscription {
        let mut ids: Vec<_> = state.in_flight.into_keys().collect();
        ids.append(&mut abandoned_deliveries);
        ids.sort_unstable();
        ids.dedup();
        if !ids.is_empty() {
            broker.release(&subscription.topic, &subscription.channel, &ids);
        }
        if subscription.channel.ends_with("#ephemeral") {
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
                let _ = broker
                    .delete_channel(&subscription.topic, &subscription.channel)
                    .await;
            }
        }
    }
    session_result
}

async fn fetch_deliveries(broker: &Broker, request: FetchRequest) -> Result<FetchResponse, String> {
    let batch = broker
        .fetch_batch_retained(
            &request.topic,
            &request.channel,
            request.max_messages as usize,
            request.max_bytes as usize,
            Duration::from_millis(request.wait_ms as u64),
            Some(Duration::from_millis(request.timeout_ms)),
        )
        .await
        .map_err(|error| error.to_string())?;
    let (deliveries, delivery_guard) = batch.into_parts();
    let deliveries = deliveries
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
        delivery_guard,
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

fn publish_command(command: &Command) -> bool {
    matches!(
        command,
        Command::Publish { .. } | Command::MultiPublish { .. } | Command::DeferredPublish { .. }
    )
}

fn delivery_is_outstanding(in_flight: &HashMap<u64, Instant>, id: u64) -> bool {
    in_flight.contains_key(&id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pending_fetch_survives_an_unrelated_ready_branch() {
        let request = FetchRequest {
            topic: "events".into(),
            channel: "workers".into(),
            timeout_ms: 1_000,
            max_messages: 1,
            max_bytes: 1024,
            wait_ms: 100,
        };
        let future = Box::pin(async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(FetchResponse {
                deliveries: Vec::new(),
                delivery_guard: DeliveryGuard::default(),
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
        assert!(response.deliveries.is_empty());
    }

    #[test]
    fn duplicate_delivery_is_suppressed_while_in_flight() {
        let mut in_flight = HashMap::new();
        in_flight.insert(7, Instant::now());
        assert!(delivery_is_outstanding(&in_flight, 7));

        in_flight.clear();
        assert!(!delivery_is_outstanding(&in_flight, 7));
        assert!(!delivery_is_outstanding(&in_flight, 8));
    }
}
