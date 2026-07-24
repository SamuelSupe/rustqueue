use super::*;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve_kodo_compat(
    config: Arc<Config>,
    broker: Arc<Broker>,
    metrics: Arc<Metrics>,
    accepting: Arc<AtomicBool>,
    delivering: Arc<AtomicBool>,
    publish_admission: Arc<PublishAdmission>,
    subscriptions: SubscriptionRegistry,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let Some(address) = config.network.kodo_http_address else {
        wait_for_shutdown(shutdown).await;
        return Ok(());
    };
    let state = app_state(
        config,
        broker,
        metrics,
        accepting,
        delivering,
        publish_admission,
        subscriptions,
    )?;
    let tokens = state.tokens.clone();
    let token_shutdown = shutdown.clone();
    let router = Router::new()
        .route("/ping", get(ping))
        .route("/stats", get(stats))
        .route("/channel/delete", post(delete_idle_channel_compat))
        .layer(middleware::from_fn(nsq_content_negotiation))
        .with_state(state);
    let listener = TcpListener::bind(address).await?;
    info!(%address, "Kodo compatibility HTTP API listening");
    let reloader = tokio::spawn(tokens.reload(token_shutdown));
    let result = axum::serve(listener, router)
        .with_graceful_shutdown(wait_for_shutdown(shutdown))
        .await;
    reloader.abort();
    result?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustqueue_queue::BrokerConfig;

    #[tokio::test]
    async fn compatibility_delete_is_idle_only_without_an_http_token() {
        let root = tempfile::tempdir().unwrap();
        let broker = Arc::new(
            Broker::open(BrokerConfig {
                data_path: root.path().into(),
                ..BrokerConfig::default()
            })
            .unwrap(),
        );
        broker.create_channel("events", "workers").await.unwrap();
        let metrics = Arc::new(Metrics::default());
        let state = AppState {
            config: Arc::new(Config::default()),
            broker: Arc::clone(&broker),
            metrics: Arc::clone(&metrics),
            tokens: TokenSet::from_config(&Config::default()).unwrap(),
            accepting: Arc::new(AtomicBool::new(true)),
            delivering: Arc::new(AtomicBool::new(true)),
            publish_admission: Arc::new(PublishAdmission::new(1024, metrics)),
            subscriptions: SubscriptionRegistry::default(),
            started_at: 1,
        };

        let response = delete_idle_channel_compat(
            State(state.clone()),
            Query(ChannelQuery {
                topic: "events".into(),
                channel: "workers".into(),
            }),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert!(broker.stats().topics[0].channels.is_empty());

        let missing = delete_idle_channel_compat(
            State(state),
            Query(ChannelQuery {
                topic: "events".into(),
                channel: "workers".into(),
            }),
        )
        .await;
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(missing.into_body(), usize::MAX)
            .await
            .unwrap();
        assert!(String::from_utf8_lossy(&body).contains("CHANNEL_NOT_FOUND"));
    }
}
