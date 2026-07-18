use anyhow::{bail, Context};
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct Config {
    pub address: SocketAddr,
    pub namespace: String,
    pub queue_name: String,
    pub console_token_file: PathBuf,
    pub static_dir: PathBuf,
    pub poll_interval: Duration,
    pub catalog_refresh_interval: Duration,
    pub history_capacity: usize,
    pub broker_http_port: u16,
    pub management_enabled: bool,
    pub management_unlock: Duration,
    pub tombstone_ttl: Duration,
}

impl Config {
    pub fn from_environment() -> anyhow::Result<Self> {
        let poll_seconds = parse("RUSTQUEUE_CONSOLE_POLL_SECONDS", 2u64)?;
        if !(1..=5).contains(&poll_seconds) {
            bail!("RUSTQUEUE_CONSOLE_POLL_SECONDS must be in 1..=5");
        }
        let history_minutes = parse("RUSTQUEUE_CONSOLE_HISTORY_MINUTES", 15usize)?;
        if !(1..=60).contains(&history_minutes) {
            bail!("RUSTQUEUE_CONSOLE_HISTORY_MINUTES must be in 1..=60");
        }
        let catalog_refresh_seconds = parse("RUSTQUEUE_CONSOLE_CATALOG_REFRESH_SECONDS", 30u64)?;
        if catalog_refresh_seconds < poll_seconds || catalog_refresh_seconds > 300 {
            bail!("RUSTQUEUE_CONSOLE_CATALOG_REFRESH_SECONDS must be in poll_seconds..=300");
        }
        let management_unlock_seconds =
            parse("RUSTQUEUE_CONSOLE_MANAGEMENT_UNLOCK_SECONDS", 1800u64)?;
        if !(60..=1800).contains(&management_unlock_seconds) {
            bail!("RUSTQUEUE_CONSOLE_MANAGEMENT_UNLOCK_SECONDS must be in 60..=1800");
        }
        let tombstone_seconds = parse("RUSTQUEUE_CONSOLE_TOMBSTONE_SECONDS", 600u64)?;
        if !(60..=86_400).contains(&tombstone_seconds) {
            bail!("RUSTQUEUE_CONSOLE_TOMBSTONE_SECONDS must be in 60..=86400");
        }
        Ok(Self {
            address: env::var("RUSTQUEUE_CONSOLE_ADDRESS")
                .unwrap_or_else(|_| "0.0.0.0:4180".into())
                .parse()
                .context("parse RUSTQUEUE_CONSOLE_ADDRESS")?,
            namespace: env::var("POD_NAMESPACE").unwrap_or_else(|_| "default".into()),
            queue_name: env::var("RUSTQUEUE_NAME").unwrap_or_else(|_| "rustqueue".into()),
            console_token_file: env::var("RUSTQUEUE_CONSOLE_TOKEN_FILE")
                .unwrap_or_else(|_| "/run/secrets/rustqueue/console-token".into())
                .into(),
            static_dir: env::var("RUSTQUEUE_CONSOLE_STATIC_DIR")
                .unwrap_or_else(|_| "/usr/share/rustqueue-console".into())
                .into(),
            poll_interval: Duration::from_secs(poll_seconds),
            catalog_refresh_interval: Duration::from_secs(catalog_refresh_seconds),
            history_capacity: history_minutes * 60 / poll_seconds as usize,
            broker_http_port: parse("RUSTQUEUE_BROKER_HTTP_PORT", 4151u16)?,
            management_enabled: parse("RUSTQUEUE_CONSOLE_MANAGEMENT_ENABLED", false)?,
            management_unlock: Duration::from_secs(management_unlock_seconds),
            tombstone_ttl: Duration::from_secs(tombstone_seconds),
        })
    }
}

fn parse<T>(name: &str, default: T) -> anyhow::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match env::var(name) {
        Ok(value) => value.parse().with_context(|| format!("parse {name}")),
        Err(_) => Ok(default),
    }
}
