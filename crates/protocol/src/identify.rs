use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct IdentifyRequest {
    #[serde(default)]
    pub client_id: String,
    #[serde(default)]
    pub hostname: String,
    #[serde(default)]
    pub feature_negotiation: bool,
    pub heartbeat_interval: Option<i64>,
    pub output_buffer_size: Option<i64>,
    pub output_buffer_timeout: Option<i64>,
    #[serde(default)]
    pub tls_v1: bool,
    #[serde(default)]
    pub snappy: bool,
    #[serde(default)]
    pub deflate: bool,
    pub deflate_level: Option<i32>,
    pub sample_rate: Option<i32>,
    #[serde(default)]
    pub user_agent: String,
    pub msg_timeout: Option<i64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct IdentifyResponse {
    pub max_rdy_count: u64,
    pub version: String,
    pub max_msg_timeout: i64,
    pub msg_timeout: i64,
    pub tls_v1: bool,
    pub deflate: bool,
    pub deflate_level: i32,
    pub max_deflate_level: i32,
    pub snappy: bool,
    pub sample_rate: i32,
    pub auth_required: bool,
    pub output_buffer_size: i64,
    pub output_buffer_timeout: i64,
}
