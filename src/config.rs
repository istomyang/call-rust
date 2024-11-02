use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub connector_config: ConnectorConfig,
    pub transmitter_config: TransmitterConfig,
    pub cluster_config: ClusterConfig,
    pub tracer_config: TracerConfig,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub use_cluster: bool,
    pub udp_port: u32,
    pub etcd_addr: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConnectorConfig {
    pub use_tcp: bool,
    pub tcp_bind_addr: String,
    pub use_udp: bool,
    pub udp_bind_addr: String,
    pub msg_buf_size: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TransmitterConfig {
    pub use_memory: bool,
    pub use_redis: bool,
    pub redis_uri: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TracerConfig {
    pub use_tracer: bool,

    pub use_mongo: bool,
    pub mongo_url: String,
    pub use_csv: bool,
    pub cvs_file_path: String,
    pub use_pg: bool,
    pub pg_config: String,

    pub batch_interval_sec: u8,
    pub batch_size: usize,
}

impl Config {
    pub fn from_yaml(path: String) -> Result<Self, anyhow::Error> {
        let data = std::fs::read_to_string(path).unwrap();
        serde_yml::from_str(&data).map_err(anyhow::Error::from)
    }

    pub fn from_json(path: String) -> Result<Self, anyhow::Error> {
        let data = std::fs::read_to_string(path).unwrap();
        serde_json::from_str(&data).map_err(anyhow::Error::from)
    }
}
