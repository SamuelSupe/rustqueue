use bytes::Bytes;
use rustqueue_queue::{Broker, BrokerConfig};
use std::time::Duration;

#[tokio::test]
async fn persists_and_delivers_a_hundred_mebibyte_message() {
    let directory = tempfile::tempdir().unwrap();
    let config = BrokerConfig {
        data_path: directory.path().into(),
        max_segment_bytes: 256 * 1024 * 1024,
        max_message_bytes: 100 * 1024 * 1024,
        storage_feature_level: 2,
        ..BrokerConfig::default()
    };
    let broker = Broker::open(config.clone()).unwrap();
    broker.create_topic("large").await.unwrap();
    broker.create_channel("large", "workers").await.unwrap();

    let body = Bytes::from(vec![0x5a; 100 * 1024 * 1024]);
    broker
        .publish("large", vec![body.clone()], Duration::ZERO)
        .await
        .unwrap();
    broker.flush().await.unwrap();
    drop(broker);

    let broker = Broker::open(BrokerConfig {
        max_message_bytes: 20 * 1024 * 1024,
        ..config
    })
    .unwrap();
    let delivery = broker
        .next_message("large", "workers", None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivery.body.as_ref(), body.as_ref());
}
