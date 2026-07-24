use serde::{Deserialize, Serialize};
use std::net::IpAddr;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct BrokerEndpoint {
    pub address: IpAddr,
    pub http_port: u16,
}

impl BrokerEndpoint {
    pub fn registry_url(&self) -> String {
        match self.address {
            IpAddr::V4(address) => format!("http://{address}:{}/v1/registry", self.http_port),
            IpAddr::V6(address) => format!("http://[{address}]:{}/v1/registry", self.http_port),
        }
    }

    pub fn registry_head_url(&self) -> String {
        match self.address {
            IpAddr::V4(address) => format!("http://{address}:{}/v1/registry/head", self.http_port),
            IpAddr::V6(address) => {
                format!("http://[{address}]:{}/v1/registry/head", self.http_port)
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct BrokerRegistry {
    pub format: u8,
    pub revision: u64,
    pub node_id: u64,
    pub ready: bool,
    #[serde(default)]
    pub publish_ready: bool,
    #[serde(default)]
    pub consume_ready: bool,
    pub broadcast_address: String,
    pub tcp_port: u16,
    pub http_port: u16,
    #[serde(default)]
    pub stored_messages: u64,
    #[serde(default)]
    pub depth: u64,
    #[serde(default)]
    pub in_flight: u64,
    pub topics: Vec<RegistryTopic>,
    #[serde(default)]
    pub compatibility: Option<CompatibilityReport>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct BrokerRegistryHead {
    pub format: u8,
    pub revision: u64,
    pub node_id: u64,
    pub ready: bool,
    pub publish_ready: bool,
    pub consume_ready: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CompatibilityReport {
    pub binary: BinaryCapabilities,
    pub storage: CompatibilityState,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct BinaryCapabilities {
    pub binary_version: String,
    pub data_format: u32,
    pub minimum_reader_feature_level: u32,
    pub maximum_reader_feature_level: u32,
    pub maximum_writer_feature_level: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CompatibilityState {
    pub data_format: u32,
    pub active_writer_feature_level: u32,
    pub minimum_reader_feature_level: u32,
    pub generation: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct RegistryTopic {
    pub name: String,
    #[serde(default)]
    pub paused: bool,
    #[serde(default)]
    pub channels: Vec<String>,
    #[serde(default)]
    pub stored_messages: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Producer {
    pub remote_address: String,
    pub hostname: String,
    pub broadcast_address: String,
    pub tcp_port: u16,
    pub http_port: u16,
    pub version: String,
    pub node_id: u64,
}

impl Producer {
    pub fn from_registry(registry: &BrokerRegistry) -> Self {
        Self {
            remote_address: format!("{}:{}", registry.broadcast_address, registry.tcp_port),
            hostname: registry.broadcast_address.clone(),
            broadcast_address: registry.broadcast_address.clone(),
            tcp_port: registry.tcp_port,
            http_port: registry.http_port,
            version: env!("CARGO_PKG_VERSION").into(),
            node_id: registry.node_id,
        }
    }

    pub fn gateway(address: String, ordinal: usize) -> Self {
        let tcp_port = [4150, 4152, 4153].get(ordinal).copied().unwrap_or(4150);
        let http_port = [4151, 4154, 4155].get(ordinal).copied().unwrap_or(4151);
        Self {
            remote_address: format!("{address}:{tcp_port}"),
            hostname: address.clone(),
            broadcast_address: address,
            tcp_port,
            http_port,
            version: env!("CARGO_PKG_VERSION").into(),
            node_id: 1_000_000u64.saturating_add(ordinal as u64),
        }
    }
}
