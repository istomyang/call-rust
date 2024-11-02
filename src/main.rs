use std::env;

use clap::Parser;
use config::Config;
use tokio::{signal, sync::oneshot};

mod app;
mod cli;
mod cluster;
mod config;
mod connect;
mod load_balance;
mod trace;
mod transmit;

#[tokio::main]
async fn main() {
    println!("Hello, call-rust!");

    let args: Vec<String> = env::args().collect();

    let config = if args.len() > 1 {
        let cli = cli::Cli::parse();
        Config::try_from(cli).unwrap()
    } else {
        default_config()
    };

    let mut app = app::App::new(config);

    let (close_tx, close_rx) = oneshot::channel();
    tokio::spawn(async move {
        signal::ctrl_c().await.unwrap();
        close_tx.send(()).unwrap();
    });

    app.run(close_rx).await.unwrap();
}

fn default_config() -> Config {
    config::Config {
        connector_config: config::ConnectorConfig {
            use_tcp: true,
            tcp_bind_addr: "127.0.0.1:8080".to_string(),
            use_udp: true,
            udp_bind_addr: "127.0.0.1:8081".to_string(),
            msg_buf_size: 1024,
        },
        transmitter_config: config::TransmitterConfig {
            use_memory: false,
            use_redis: false,
            redis_uri: "redis://127.0.0.1:6379".to_string(),
        },
        cluster_config: config::ClusterConfig {
            use_cluster: true,
            udp_port: 8082,
            etcd_addr: "127.0.0.1:2379".to_string(),
        },
        tracer_config: config::TracerConfig {
            use_tracer: true,
            use_csv: true,
            cvs_file_path: "./trace.csv".to_string(),
            use_mongo: false,
            mongo_url: "mongodb://127.0.0.1:27017".to_string(),
            use_pg: false,
            pg_config: "".to_string(),
            batch_interval_sec: 5,
            batch_size: 100,
        },
    }
}
