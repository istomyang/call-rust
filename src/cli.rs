use clap::{command, Parser, Subcommand};

use crate::config;

#[derive(Debug, Parser)] // requires `derive` feature
#[command(name = "call-rust")]
#[command(
    about = "An alternative solution for remote-calling.",
    long_about = "call-rust is an alternative solution for remote-calling, compare to grpc."
)]
pub struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Run app with config.
    Config {
        #[arg(short, long, value_name = "file path")]
        yaml: Option<String>,

        #[arg(short, long, value_name = "file path")]
        json: Option<String>,
    },

    /// Run app with args.
    Args {
        #[arg(short, long)]
        connector_use_tcp: Option<bool>,
        connector_tcp_bind_addr: Option<String>,
        connector_use_udp: Option<bool>,
        connector_udp_bind_addr: Option<String>,
        connector_msg_buf_size: Option<usize>,

        transmitter_use_memory: Option<bool>,
        transmitter_use_redis: Option<bool>,
        transmitter_redis_url: Option<String>,

        cluster_use_cluster: Option<bool>,
        cluster_udp_port: Option<u32>,
        cluster_etcd_addr: Option<String>,

        tracer_use_tracer: Option<bool>,
        tracer_use_mongo: Option<bool>,
        tracer_mongo_url: Option<String>,
        tracer_use_csv: Option<bool>,
        tracer_cvs_file_path: Option<String>,
        tracer_use_pg: Option<bool>,
        tracer_pg_config: Option<String>,
        tracer_batch_interval_sec: Option<u8>,
        tracer_batch_size: Option<usize>,
    },
}

impl TryFrom<Cli> for config::Config {
    type Error = anyhow::Error;

    fn try_from(cli: Cli) -> Result<Self, Self::Error> {
        match cli.command {
            Commands::Config { yaml, json } => {
                if let Some(yaml) = yaml {
                    Ok(config::Config::from_yaml(yaml)?)
                } else if let Some(json) = json {
                    Ok(config::Config::from_json(json)?)
                } else {
                    Err(anyhow::anyhow!("yaml or json must be set"))
                }
            }
            Commands::Args {
                connector_use_tcp,
                connector_tcp_bind_addr,
                connector_use_udp,
                connector_udp_bind_addr,
                connector_msg_buf_size,
                transmitter_use_memory,
                transmitter_use_redis,
                transmitter_redis_url,
                cluster_use_cluster,
                cluster_udp_port,
                cluster_etcd_addr,
                tracer_use_tracer,
                tracer_use_mongo,
                tracer_mongo_url,
                tracer_use_csv,
                tracer_cvs_file_path,
                tracer_use_pg,
                tracer_pg_config,
                tracer_batch_interval_sec,
                tracer_batch_size,
            } => {
                let config = config::Config {
                    connector_config: config::ConnectorConfig {
                        use_tcp: connector_use_tcp.unwrap_or(false),
                        tcp_bind_addr: connector_tcp_bind_addr
                            .unwrap_or("0.0.0.0:3000".to_string()),
                        use_udp: connector_use_udp.unwrap_or(true),
                        udp_bind_addr: connector_udp_bind_addr
                            .unwrap_or("0.0.0.0:3001".to_string()),
                        msg_buf_size: connector_msg_buf_size.unwrap_or(1024),
                    },
                    transmitter_config: config::TransmitterConfig {
                        use_memory: transmitter_use_memory.unwrap_or(true),
                        use_redis: transmitter_use_redis.unwrap_or(false),
                        redis_uri: transmitter_redis_url.unwrap_or("".to_string()),
                    },
                    cluster_config: config::ClusterConfig {
                        use_cluster: cluster_use_cluster.unwrap_or(false),
                        udp_port: cluster_udp_port.unwrap_or(3002),
                        etcd_addr: cluster_etcd_addr.unwrap_or("127.0.0.1:2379".to_string()),
                    },
                    tracer_config: config::TracerConfig {
                        use_tracer: tracer_use_tracer.unwrap_or(false),
                        use_mongo: tracer_use_mongo.unwrap_or(false),
                        mongo_url: tracer_mongo_url.unwrap_or("".to_string()),
                        use_csv: tracer_use_csv.unwrap_or(false),
                        cvs_file_path: tracer_cvs_file_path.unwrap_or("".to_string()),
                        use_pg: tracer_use_pg.unwrap_or(false),
                        pg_config: tracer_pg_config.unwrap_or("".to_string()),
                        batch_interval_sec: tracer_batch_interval_sec.unwrap_or(60 * 5),
                        batch_size: tracer_batch_size.unwrap_or(100),
                    },
                };
                Ok(config)
            }
        }
    }
}
