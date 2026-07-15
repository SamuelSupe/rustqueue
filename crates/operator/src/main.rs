use kube::CustomResourceExt;
use rustqueue_operator::RustQueueCluster;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "rustqueue_operator=info".into()),
        )
        .json()
        .init();
    match std::env::args().nth(1).as_deref() {
        Some("crd") => {
            print!("{}", serde_yaml::to_string(&RustQueueCluster::crd())?);
            Ok(())
        }
        Some(command) if command != "run" => anyhow::bail!("unknown command {command}"),
        _ => rustqueue_operator::run().await,
    }
}
