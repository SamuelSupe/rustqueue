use rustqueue_queue::{Broker, BrokerConfig};
use std::time::Duration;

#[tokio::test]
async fn persists_and_delivers_a_twenty_mebibyte_message() {
    let directory = tempfile::tempdir().unwrap();
    let broker = Broker::open(BrokerConfig {
        data_path: directory.path().into(),
        max_message_bytes: 32 * 1024 * 1024,
        ..BrokerConfig::default()
    })
    .unwrap();
    broker.create_topic("large").await.unwrap();
    broker.create_channel("large", "workers").await.unwrap();

    let body = vec![0x5a; 20 * 1024 * 1024];
    broker
        .publish("large", vec![body.clone()], Duration::ZERO)
        .await
        .unwrap();
    let delivery = broker
        .next_message("large", "workers", None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivery.body.as_ref(), body);
}
