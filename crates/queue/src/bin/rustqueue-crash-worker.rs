use rustqueue_queue::{Broker, BrokerConfig};
use std::path::Path;
use std::time::Duration;

fn config(path: &Path) -> BrokerConfig {
    BrokerConfig {
        data_path: path.into(),
        node_id: 41,
        max_segment_bytes: 256,
        max_message_bytes: 1024,
        bootstrap_retention: Duration::ZERO,
        ..BrokerConfig::default()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut arguments = std::env::args().skip(1);
    let scenario = arguments
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing scenario"))?;
    let path = arguments
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing data path"))?;
    let broker = Broker::open(config(Path::new(&path)))?;
    match scenario.as_str() {
        "publish" => {
            broker
                .publish("events", vec![b"ambiguous".to_vec()], Duration::ZERO)
                .await?;
        }
        "finish" => {
            let message = broker
                .next_message("events", "workers", None)
                .await?
                .ok_or_else(|| anyhow::anyhow!("missing message to finish"))?;
            broker.finish("events", "workers", message.id).await?;
        }
        "checkpoint" => broker.checkpoint().await?,
        "gc" => {
            broker.compact().await?;
        }
        _ => anyhow::bail!("unknown crash scenario {scenario}"),
    }
    anyhow::bail!("crash failpoint did not trigger for scenario {scenario}")
}
