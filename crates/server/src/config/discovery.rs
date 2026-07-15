use super::read_optional_secret;
use anyhow::{bail, Context};
use libp2p::Multiaddr;
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DiscoveryConfig {
    pub enabled: bool,
    pub listen_address: String,
    pub seed_addresses: Vec<String>,
    pub mdns: bool,
    pub identity_file: Option<PathBuf>,
    pub join_token_file: Option<PathBuf>,
    pub announce_interval_seconds: u64,
    pub max_known_peers: usize,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            listen_address: "/ip4/0.0.0.0/tcp/4350".into(),
            seed_addresses: Vec::new(),
            mdns: true,
            identity_file: None,
            join_token_file: None,
            announce_interval_seconds: 15,
            max_known_peers: 128,
        }
    }
}

impl DiscoveryConfig {
    pub fn validate(&self, cluster_enabled: bool) -> anyhow::Result<()> {
        if !self.enabled {
            return Ok(());
        }
        if !cluster_enabled {
            bail!("cluster.discovery requires cluster mode");
        }
        if !self.mdns && self.seed_addresses.is_empty() {
            bail!("cluster.discovery requires mDNS or at least one seed address");
        }
        if !(1..=300).contains(&self.announce_interval_seconds) {
            bail!("cluster.discovery announce interval must be in 1..=300 seconds");
        }
        if !(1..=4096).contains(&self.max_known_peers) {
            bail!("cluster.discovery max_known_peers must be in 1..=4096");
        }
        parse_address(&self.listen_address, "listen address")?;
        for address in &self.seed_addresses {
            parse_address(address, "seed address")?;
        }
        let token = self
            .join_token_file
            .as_deref()
            .context("cluster.discovery.join_token_file is required")?;
        if !token.is_file() {
            bail!("discovery join token {} is not readable", token.display());
        }
        if self
            .identity_file
            .as_ref()
            .is_some_and(|path| path.is_dir())
        {
            bail!("cluster.discovery.identity_file cannot be a directory");
        }
        Ok(())
    }

    pub fn identity_path(&self, data_path: &Path) -> PathBuf {
        self.identity_file
            .clone()
            .unwrap_or_else(|| data_path.join("discovery/identity.key"))
    }

    pub fn read_join_token(&self) -> anyhow::Result<Vec<u8>> {
        let token = read_optional_secret(self.join_token_file.as_deref())?
            .context("cluster.discovery.join_token_file is required")?;
        if token.len() < 32 {
            bail!("cluster discovery join token must contain at least 32 bytes");
        }
        Ok(token.into_bytes())
    }
}

fn parse_address(value: &str, field: &str) -> anyhow::Result<Multiaddr> {
    value
        .parse()
        .with_context(|| format!("parse cluster.discovery {field} {value}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enabled_discovery_requires_a_long_join_token() {
        let directory = tempfile::tempdir().unwrap();
        let token = directory.path().join("join.token");
        std::fs::write(&token, "short").unwrap();
        let config = DiscoveryConfig {
            enabled: true,
            mdns: false,
            seed_addresses: vec!["/ip4/127.0.0.1/tcp/4350".into()],
            join_token_file: Some(token.clone()),
            ..DiscoveryConfig::default()
        };
        config.validate(true).unwrap();
        assert!(config.read_join_token().is_err());

        std::fs::write(token, "0123456789abcdef0123456789abcdef").unwrap();
        assert_eq!(config.read_join_token().unwrap().len(), 32);
    }
}
