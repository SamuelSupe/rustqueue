use anyhow::{bail, Context};
use clap::{Parser, Subcommand, ValueEnum};
use futures::{stream, StreamExt};
use k8s_openapi::api::core::v1::{Pod, Secret};
use kube::api::{Api, ListParams, Patch, PatchParams};
use kube::{Client, ResourceExt};
use rustqueue_operator::RustQueue;
use serde_json::{json, Value};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Parser)]
#[command(version, about = "Operate a Kubernetes RustQueue cluster")]
struct Cli {
    #[arg(
        short = 'n',
        long,
        env = "RUSTQUEUE_NAMESPACE",
        default_value = "default"
    )]
    namespace: String,
    #[arg(long, env = "RUSTQUEUE_NAME", default_value = "rustqueue")]
    name: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Status,
    Brokers,
    Stats,
    Scrub,
    Maintenance {
        broker: String,
        #[arg(value_enum)]
        action: MaintenanceAction,
    },
    Rollout {
        #[command(subcommand)]
        action: RolloutAction,
    },
    Storage {
        size: String,
    },
}

#[derive(Clone, ValueEnum)]
enum MaintenanceAction {
    Enable,
    Disable,
}

#[derive(Subcommand)]
enum RolloutAction {
    Pause,
    Resume,
    Approve,
    Retry,
    Rollback { image: String },
    Forward,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let client = Client::try_default().await?;
    let clusters = Api::<RustQueue>::namespaced(client.clone(), &cli.namespace);
    match cli.command {
        Command::Status => {
            let cluster = clusters.get(&cli.name).await?;
            println!("{}", serde_json::to_string_pretty(&cluster)?);
        }
        Command::Brokers => brokers(&client, &cli.namespace, &cli.name).await?,
        Command::Stats => fanout(&client, &cli.namespace, &cli.name, Fanout::Stats).await?,
        Command::Scrub => fanout(&client, &cli.namespace, &cli.name, Fanout::Scrub).await?,
        Command::Maintenance { broker, action } => {
            let enabled = matches!(action, MaintenanceAction::Enable);
            patch_spec(
                &clusters,
                &cli.name,
                json!({"maintenance": {"broker": broker, "enabled": enabled}}),
            )
            .await?;
            println!("maintenance request submitted");
        }
        Command::Rollout { action } => rollout(&clusters, &cli.name, action).await?,
        Command::Storage { size } => {
            patch_spec(&clusters, &cli.name, json!({"storageSize": size})).await?;
            println!("storage expansion request submitted");
        }
    }
    Ok(())
}

async fn rollout(api: &Api<RustQueue>, name: &str, action: RolloutAction) -> anyhow::Result<()> {
    let patch = match action {
        RolloutAction::Pause => json!({"rollout": {"paused": true}}),
        RolloutAction::Resume => json!({"rollout": {"paused": false}}),
        RolloutAction::Retry => json!({"rollout": {
            "paused": false,
            "retryNonce": unix_nanos().to_string(),
        }}),
        RolloutAction::Rollback { image } => {
            if image.trim().is_empty() {
                bail!("rollback image cannot be empty");
            }
            json!({"rollout": {"paused": false, "rollbackToImage": image}})
        }
        RolloutAction::Forward => {
            json!({"rollout": {"paused": false, "rollbackToImage": null}})
        }
        RolloutAction::Approve => {
            let cluster = api.get(name).await?;
            let revision = cluster
                .status
                .as_ref()
                .and_then(|status| status.current_operation.as_ref())
                .filter(|operation| operation.phase == "AwaitingCanaryApproval")
                .map(|operation| operation.revision.clone())
                .context("cluster is not awaiting canary approval")?;
            json!({"rollout": {"approvedRevision": revision}})
        }
    };
    patch_spec(api, name, patch).await?;
    println!("rollout request submitted");
    Ok(())
}

async fn patch_spec(api: &Api<RustQueue>, name: &str, spec: Value) -> anyhow::Result<()> {
    api.patch(
        name,
        &PatchParams::default(),
        &Patch::Merge(json!({"spec": spec})),
    )
    .await?;
    Ok(())
}

async fn brokers(client: &Client, namespace: &str, name: &str) -> anyhow::Result<()> {
    let pods = broker_pods(client, namespace, name).await?;
    println!("BROKER\tREADY\tNODE\tIP\tREVISION");
    for pod in pods {
        let ready = is_ready(&pod);
        let node = pod
            .spec
            .as_ref()
            .and_then(|spec| spec.node_name.as_deref())
            .unwrap_or("-");
        let ip = pod
            .status
            .as_ref()
            .and_then(|status| status.pod_ip.as_deref())
            .unwrap_or("-");
        let revision = pod
            .metadata
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.get("rustqueue.io/revision"))
            .map(String::as_str)
            .unwrap_or("-");
        println!(
            "{}\t{}\t{}\t{}\t{}",
            pod.name_any(),
            ready,
            node,
            ip,
            revision
        );
    }
    Ok(())
}

enum Fanout {
    Stats,
    Scrub,
}

async fn fanout(
    client: &Client,
    namespace: &str,
    name: &str,
    operation: Fanout,
) -> anyhow::Result<()> {
    let cluster = Api::<RustQueue>::namespaced(client.clone(), namespace)
        .get(name)
        .await?;
    let secret_name = cluster
        .spec
        .registry_secret_name
        .unwrap_or_else(|| format!("{name}-auth"));
    let secret = Api::<Secret>::namespaced(client.clone(), namespace)
        .get(&secret_name)
        .await?;
    let token_key = match operation {
        Fanout::Stats => "registry-token",
        Fanout::Scrub => "admin-token",
    };
    let token = secret_token(&secret, token_key)?;
    let pods = broker_pods(client, namespace, name).await?;
    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let is_scrub = matches!(operation, Fanout::Scrub);
    let results = stream::iter(pods.into_iter().map(|pod| {
        let http = http.clone();
        let token = token.clone();
        async move {
            let pod_name = pod.name_any();
            let ip = pod
                .status
                .as_ref()
                .and_then(|status| status.pod_ip.as_deref())
                .context("Pod has no IP")?;
            let path = if is_scrub {
                "v1/storage/scrub"
            } else {
                "v1/stats"
            };
            let url = format!("{}/{}", origin(ip), path);
            let request = if is_scrub {
                http.post(url)
            } else {
                http.get(url)
            };
            let response = request
                .bearer_auth(token)
                .send()
                .await?
                .error_for_status()?
                .json::<Value>()
                .await?;
            Ok::<_, anyhow::Error>((pod_name, response))
        }
    }))
    .buffer_unordered(16)
    .collect::<Vec<_>>()
    .await;
    let mut failed = false;
    for result in results {
        match result {
            Ok((pod, response)) => println!("{pod}\t{}", serde_json::to_string(&response)?),
            Err(error) => {
                failed = true;
                eprintln!("broker request failed: {error:#}");
            }
        }
    }
    if failed {
        bail!("one or more Broker operations failed");
    }
    Ok(())
}

async fn broker_pods(client: &Client, namespace: &str, name: &str) -> anyhow::Result<Vec<Pod>> {
    let selector = format!("app.kubernetes.io/instance={name},app.kubernetes.io/component=broker");
    let mut pods = Api::<Pod>::namespaced(client.clone(), namespace)
        .list(&ListParams::default().labels(&selector))
        .await?
        .items;
    pods.sort_by_key(ResourceExt::name_any);
    Ok(pods)
}

fn secret_token(secret: &Secret, key: &str) -> anyhow::Result<String> {
    let bytes = secret
        .data
        .as_ref()
        .and_then(|data| data.get(key))
        .with_context(|| format!("Secret {} is missing {key}", secret.name_any()))?;
    Ok(String::from_utf8(bytes.0.clone())?.trim().to_owned())
}

fn is_ready(pod: &Pod) -> bool {
    pod.status
        .as_ref()
        .and_then(|status| status.conditions.as_ref())
        .is_some_and(|conditions| {
            conditions
                .iter()
                .any(|condition| condition.type_ == "Ready" && condition.status == "True")
        })
}

fn origin(ip: &str) -> String {
    if ip.contains(':') {
        format!("http://[{ip}]:4151")
    } else {
        format!("http://{ip}:4151")
    }
}

fn unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}
