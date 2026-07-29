use super::*;

type FetchFuture<'a> =
    Pin<Box<dyn Future<Output = Result<FetchResponse, BrokerError>> + Send + 'a>>;

struct PendingFetch<'a> {
    request: FetchRequest,
    future: FetchFuture<'a>,
}

const CHANNEL_OP_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);

async fn poll_pending_fetch(
    pending: &mut Option<PendingFetch<'_>>,
) -> Result<FetchResponse, BrokerError> {
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
    subscriptions: SubscriptionRegistry,
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
    let (channel_ops, mut channel_op_results, channel_ops_task) = start_channel_ops(broker.clone());

    let session_result: anyhow::Result<()> = async {
        loop {
            let expired = expire_client_deadlines(
                &mut state.in_flight,
                &mut state.in_flight_deadlines,
                Instant::now(),
            );
            if expired {
                if let Some(subscription) = state.subscription.as_ref() {
                    broker.expire_channel_in_flight(
                        &subscription.topic,
                        &subscription.channel,
                    )
                    .await?;
                }
                state.update_subscription_flow();
            }
            if state.closing && state.in_flight.is_empty() {
                break;
            }
            if let Some(command) = pending.take() {
                if !accepting.load(Ordering::Acquire) && publish_command(&command.command) {
                    if config.limits.disconnect_on_retriable_publish_error {
                        break;
                    }
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
                    if config.limits.disconnect_on_retriable_publish_error {
                        break;
                    }
                    write_error_timed(
                        &mut writer,
                        state.heartbeat,
                        "E_THROTTLED",
                        "local disk is above its publish watermark",
                    )
                    .await?;
                    continue;
                }
                if !broker.storage_healthy() && publish_command(&command.command) {
                    metrics.storage_errors.fetch_add(1, Ordering::Relaxed);
                    write_error_timed(
                        &mut writer,
                        state.heartbeat,
                        "E_PUB_RETRY",
                        "local storage was unavailable before publish",
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
                        &subscriptions,
                        &channel_ops,
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

            state.update_subscription_flow();
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
            let in_flight_deadline = next_client_deadline(&state.in_flight_deadlines);

            tokio::select! {
                command = command_rx.recv() => {
                    let Some(command) = command else {
                        break;
                    };
                    let command = match command {
                        Ok(command) => command,
                        Err(CommandReadError::Io(_)) => break,
                        Err(CommandReadError::Protocol { code, detail }) => {
                            if disconnect_on_retriable_protocol_error(config, code) {
                                break;
                            }
                            write_error_timed(&mut writer, state.heartbeat, code, &detail).await?;
                            break;
                        }
                    };
                    if !accepting.load(Ordering::Acquire) && publish_command(&command.command) {
                        if config.limits.disconnect_on_retriable_publish_error {
                            break;
                        }
                        write_error_timed(&mut writer, state.heartbeat, "E_DRAINING", "broker is draining").await?;
                        continue;
                    }
                    if !publish_admission.storage_ready() && publish_command(&command.command) {
                        if config.limits.disconnect_on_retriable_publish_error {
                            break;
                        }
                        write_error_timed(&mut writer, state.heartbeat, "E_THROTTLED", "local disk is above its publish watermark").await?;
                        continue;
                    }
                    if !broker.storage_healthy() && publish_command(&command.command) {
                        metrics.storage_errors.fetch_add(1, Ordering::Relaxed);
                        write_error_timed(
                            &mut writer,
                            state.heartbeat,
                            "E_PUB_RETRY",
                            "local storage was unavailable before publish",
                        )
                        .await?;
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
                            &subscriptions,
                            &channel_ops,
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
                completion = channel_op_results.recv(), if !state.pending_channel_ops.is_empty() => {
                    let Some(completion) = completion else {
                        anyhow::bail!("channel operation pipeline stopped");
                    };
                    if let Err((code, error)) =
                        apply_channel_op_completion(completion, &mut state, metrics)
                    {
                        write_broker_error(&mut writer, code, error).await?;
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
                            let write_timeout = delivery_write_timeout(state.heartbeat);
                            let visibility_timeout = delivery_visibility_timeout(
                                state.message_timeout,
                                state.output_buffer_timeout,
                            );
                            let handoff_timeout = write_timeout
                                .saturating_mul(deliveries.len() as u32)
                                .max(visibility_timeout);
                            let handoff_deadline = Instant::now() + handoff_timeout;
                            let delivery_tokens = deliveries
                                .iter()
                                .map(|delivery| {
                                    delivery_guard
                                        .token(delivery.id)
                                        .map(|token| (delivery.id, token))
                                        .ok_or_else(|| anyhow::anyhow!("delivery token is missing"))
                                })
                                .collect::<anyhow::Result<Vec<_>>>()?;
                            broker
                                .touch_deliveries(
                                    &delivery_topic,
                                    &delivery_channel,
                                    &delivery_tokens,
                                    Some(handoff_timeout),
                                )
                                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
                            let mut handed_off = Vec::with_capacity(deliveries.len());
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
                                    let token = delivery_guard
                                        .token(delivery.id)
                                        .ok_or_else(|| anyhow::anyhow!("delivery token is missing"))?;
                                    let deadline = renew_delivery_lease(
                                        broker,
                                        &delivery_topic,
                                        &delivery_channel,
                                        delivery.id,
                                        token,
                                        state.message_timeout,
                                    )
                                    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
                                    channel_ops
                                        .finish_sampled(
                                            delivery_topic.clone(),
                                            delivery_channel.clone(),
                                            delivery.id,
                                            token,
                                        )
                                        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
                                    state.record_delivery(
                                        delivery.id,
                                        InFlightDelivery {
                                            deadline,
                                            token,
                                        },
                                    );
                                    state.mark_channel_operation_pending(delivery.id);
                                    delivery_guard.accept(delivery.id);
                                    continue;
                                }
                                let header = encode_message_header(
                                    delivery.timestamp_ns,
                                    delivery.attempts,
                                    delivery.id,
                                    delivery.body.len(),
                                );
                                let token = delivery_guard
                                    .token(delivery.id)
                                    .ok_or_else(|| anyhow::anyhow!("delivery token is missing"))?;
                                tokio::time::timeout(
                                    write_timeout,
                                    writer.write_message_parts(&header, &delivery.body),
                                )
                                .await
                                .map_err(|_| anyhow::anyhow!("consumer delivery write timed out"))??;
                                let accepted_token = delivery_guard
                                    .accept_with_token(delivery.id)
                                    .ok_or_else(|| anyhow::anyhow!("delivery token is missing"))?;
                                debug_assert_eq!(accepted_token, token);
                                state.record_delivery(
                                    delivery.id,
                                    InFlightDelivery {
                                        deadline: handoff_deadline,
                                        token,
                                    },
                                );
                                handed_off.push((delivery.id, token));
                                if let Some(subscription) = &state.subscription {
                                    subscription.lease.observe_delivery();
                                }
                                metrics.delivered_messages.fetch_add(1, Ordering::Relaxed);
                            }
                            if !handed_off.is_empty() {
                                let deadline = Instant::now() + visibility_timeout;
                                broker
                                    .touch_deliveries(
                                        &delivery_topic,
                                        &delivery_channel,
                                        &handed_off,
                                        Some(visibility_timeout),
                                    )
                                    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
                                for (id, token) in handed_off {
                                    if let Some(delivery) = state.in_flight.get(&id).copied() {
                                        debug_assert_eq!(delivery.token, token);
                                        state.record_delivery(
                                            id,
                                            InFlightDelivery { deadline, ..delivery },
                                        );
                                    }
                                }
                            }
                            state.update_subscription_flow();
                        }
                        Err(error) => {
                            if broker_storage_error(&error) {
                                metrics.storage_errors.fetch_add(1, Ordering::Relaxed);
                            }
                            write_error_timed(
                                &mut writer,
                                state.heartbeat,
                                "E_DELIVERY_FAILED",
                                &error.to_string(),
                            )
                            .await?;
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
                _ = wait_for_in_flight_deadline(in_flight_deadline) => {}
            }
        }
        Ok(())
    }
    .await;
    drop(channel_ops);
    let channel_ops_drained = tokio::time::timeout(
        CHANNEL_OP_CLEANUP_TIMEOUT,
        settle_channel_op_results(&mut channel_op_results, &mut state, metrics),
    )
    .await
    .unwrap_or(false);
    if !channel_ops_drained {
        warn!(
            pending = state.pending_channel_ops.len(),
            "channel operation cleanup did not finish before the connection deadline"
        );
        channel_ops_task.abort();
    }
    let _ = channel_ops_task.await;
    // Cancelling the fetch drops its reservation guard, which only releases
    // the matching delivery tokens.
    drop(pending_fetch.take());
    if let Some(subscription) = &state.subscription {
        let deliveries =
            releaseable_in_flight_deliveries(state.in_flight, &state.pending_channel_ops);
        if !deliveries.is_empty() {
            broker.release_deliveries(&subscription.topic, &subscription.channel, &deliveries);
        }
        if subscription.channel.ends_with("#ephemeral") {
            ephemeral_consumers
                .unregister(broker, &subscription.topic, &subscription.channel)
                .await;
        }
    }
    session_result
}

async fn fetch_deliveries(
    broker: &Broker,
    request: FetchRequest,
) -> Result<FetchResponse, BrokerError> {
    let batch = broker
        .fetch_batch_retained(
            &request.topic,
            &request.channel,
            request.max_messages as usize,
            request.max_bytes as usize,
            Duration::from_millis(request.wait_ms as u64),
            Some(Duration::from_millis(request.timeout_ms)),
        )
        .await?;
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

fn delivery_is_outstanding(in_flight: &HashMap<u64, InFlightDelivery>, id: u64) -> bool {
    in_flight.contains_key(&id)
}

fn expire_client_deadlines(
    in_flight: &mut HashMap<u64, InFlightDelivery>,
    deadlines: &mut BTreeSet<(Instant, u64, u64)>,
    now: Instant,
) -> bool {
    let mut expired = false;
    while let Some((deadline, id, token)) = deadlines.first().copied() {
        if deadline > now {
            break;
        }
        deadlines.remove(&(deadline, id, token));
        if in_flight
            .get(&id)
            .is_some_and(|delivery| delivery.deadline == deadline && delivery.token == token)
        {
            in_flight.remove(&id);
            expired = true;
        }
    }
    expired
}

fn next_client_deadline(deadlines: &BTreeSet<(Instant, u64, u64)>) -> Option<Instant> {
    deadlines.first().map(|(deadline, _, _)| *deadline)
}

fn apply_channel_op_completion(
    completion: ChannelOpCompletion,
    state: &mut SessionState,
    metrics: &Metrics,
) -> Result<(), (&'static str, BrokerError)> {
    state.pending_channel_ops.remove(&completion.id);
    if let Err(error) = completion.result {
        state.restore_delivery_deadline(completion.id);
        if broker_storage_error(&error) {
            metrics.storage_errors.fetch_add(1, Ordering::Relaxed);
        }
        return Err((completion.kind.error_code(), error));
    }

    state.remove_delivery(completion.id);
    if let Some(subscription) = &state.subscription {
        match completion.kind {
            ChannelOpKind::Finish => subscription.lease.observe_finish(),
            ChannelOpKind::Requeue => subscription.lease.observe_requeue(),
            ChannelOpKind::SampleFinish => {}
        }
    }
    match completion.kind {
        ChannelOpKind::Finish => {
            metrics.finished_messages.fetch_add(1, Ordering::Relaxed);
        }
        ChannelOpKind::Requeue => {
            metrics.requeued_messages.fetch_add(1, Ordering::Relaxed);
        }
        ChannelOpKind::SampleFinish => {}
    }
    state.update_subscription_flow();
    Ok(())
}

async fn settle_channel_op_results(
    results: &mut tokio::sync::mpsc::UnboundedReceiver<ChannelOpCompletion>,
    state: &mut SessionState,
    metrics: &Metrics,
) -> bool {
    while !state.pending_channel_ops.is_empty() {
        let Some(completion) = results.recv().await else {
            return false;
        };
        let _ = apply_channel_op_completion(completion, state, metrics);
    }
    true
}

fn releaseable_in_flight_deliveries(
    in_flight: HashMap<u64, InFlightDelivery>,
    pending_channel_ops: &HashSet<u64>,
) -> Vec<(u64, u64)> {
    let mut deliveries: Vec<_> = in_flight
        .into_iter()
        .filter_map(|(id, delivery)| {
            (!pending_channel_ops.contains(&id)).then_some((id, delivery.token))
        })
        .collect();
    deliveries.sort_unstable();
    deliveries
}

async fn wait_for_in_flight_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn delivery(deadline: Instant, token: u64) -> InFlightDelivery {
        InFlightDelivery { deadline, token }
    }

    fn session_state() -> SessionState {
        SessionState {
            identified: false,
            encrypted: false,
            tls_common_name: String::new(),
            heartbeat: None,
            message_timeout: Duration::from_secs(60),
            output_buffer_size: 1,
            output_buffer_timeout: None,
            sample_rate: 0,
            sample_cursor: 0,
            auth: None,
            auth_secret: None,
            _auth_reservation: None,
            subscription: None,
            rdy: 0,
            in_flight: HashMap::new(),
            in_flight_deadlines: BTreeSet::new(),
            pending_channel_ops: HashSet::new(),
            closing: false,
            client_identity: ClientIdentity::default(),
        }
    }

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
        in_flight.insert(7, delivery(Instant::now(), 70));
        assert!(delivery_is_outstanding(&in_flight, 7));

        in_flight.clear();
        assert!(!delivery_is_outstanding(&in_flight, 7));
        assert!(!delivery_is_outstanding(&in_flight, 8));
    }

    #[test]
    fn client_deadlines_expire_without_waiting_for_another_fetch() {
        let now = Instant::now();
        let mut in_flight = HashMap::from([
            (7, delivery(now - Duration::from_millis(1), 70)),
            (8, delivery(now + Duration::from_secs(1), 80)),
        ]);
        let mut deadlines = BTreeSet::from([
            (now - Duration::from_millis(1), 7, 70),
            (now + Duration::from_secs(1), 8, 80),
        ]);
        assert!(expire_client_deadlines(&mut in_flight, &mut deadlines, now));
        assert_eq!(in_flight.keys().copied().collect::<Vec<_>>(), vec![8]);
        assert!(!expire_client_deadlines(
            &mut in_flight,
            &mut deadlines,
            now
        ));
    }

    #[test]
    fn pending_channel_operations_unschedule_and_restore_the_delivery_deadline() {
        let now = Instant::now();
        let mut state = session_state();
        let deadline = now + Duration::from_secs(1);
        state.record_delivery(7, delivery(deadline, 70));
        assert_eq!(
            next_client_deadline(&state.in_flight_deadlines),
            Some(deadline)
        );

        state.mark_channel_operation_pending(7);
        assert_eq!(next_client_deadline(&state.in_flight_deadlines), None);

        state.pending_channel_ops.remove(&7);
        state.restore_delivery_deadline(7);
        assert_eq!(
            next_client_deadline(&state.in_flight_deadlines),
            Some(deadline)
        );
    }

    #[test]
    fn unresolved_channel_operations_are_not_released_on_disconnect() {
        let now = Instant::now();
        let in_flight = HashMap::from([(7, delivery(now, 70)), (8, delivery(now, 80))]);
        let pending = HashSet::from([7]);

        assert_eq!(
            releaseable_in_flight_deliveries(in_flight, &pending),
            vec![(8, 80)]
        );
    }

    #[test]
    fn kodo_publish_admission_pressure_forces_producer_failover() {
        let mut config = Config::default();
        assert!(!disconnect_on_retriable_protocol_error(
            &config,
            "E_THROTTLED"
        ));
        config.limits.disconnect_on_retriable_publish_error = true;
        assert!(disconnect_on_retriable_protocol_error(
            &config,
            "E_THROTTLED"
        ));
        assert!(!disconnect_on_retriable_protocol_error(
            &config,
            "E_BAD_MESSAGE"
        ));
    }
}
