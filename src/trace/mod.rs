use std::{sync::Arc, time::SystemTime};

use anyhow::{Error, Ok, Result};
use chrono::{DateTime, Local, TimeZone, Utc};
use serde::Serialize;

use tokio::sync::mpsc::{self, Receiver, Sender};

pub mod write_csv;
pub mod write_mongo;
pub mod write_pg;

/// Trace writes request trace chain.
pub trait Trace {
    fn init(&mut self) -> Result<()>;
    async fn run(&mut self) -> Result<()>;

    fn get_commander(&mut self) -> (Sender<SendCmd>, Receiver<RecvCmd>);
}

pub enum SendCmd {
    Record(TraceBlock),
    Close,
}

pub enum RecvCmd {
    Err(Error),
}

/// Write defines different storage system.
pub(in crate::trace) trait Write {
    fn init(&mut self) -> Result<()>;
    async fn run(&mut self) -> Result<()>;

    fn send(&mut self) -> Sender<WriteSendCmd>;
    fn recv(&mut self) -> Receiver<WriteRecvCmd>;
}

pub(in crate::trace) enum WriteSendCmd {
    RecordBatch(Arc<Vec<TraceBlock>>),
    Close,
}

pub(in crate::trace) enum WriteRecvCmd {
    Err(Error),
}

#[derive(Debug, Serialize, Clone)]
pub struct TraceBlock {
    pub msg_id: String,
    pub request_id: String,
    pub created_at: SystemTime,

    pub from_service_name: String,
    pub to_service_name: String,
    pub from_service_no: String,
    pub to_service_no: String,

    pub is_request: bool,
    pub write_op: bool,
    pub success: Option<bool>,
    pub description: Option<String>,
}

impl TraceBlock {
    pub fn format_ms_date(ts_ms: i64) -> String {
        let dt = Utc.timestamp_millis_opt(ts_ms).unwrap();
        dt.with_timezone(&Local);
        dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
    }

    pub fn format_sql_date(ts_ms: i64) -> String {
        let utc_date_time = DateTime::from_timestamp(ts_ms / 1000, 0).unwrap();
        utc_date_time.to_rfc3339()
    }
}

// ==== Tracer Impl ====

use write_csv::WriteCsv;
use write_mongo::WriteMongo;
use write_pg::WritePg;

pub struct Config {
    pub use_mongo: bool,
    pub mongo_url: String,
    pub use_csv: bool,
    pub cvs_file_path: String,
    pub use_pg: bool,
    pub pg_config: String,

    pub batch_interval_sec: u8,
    pub batch_size: usize,
}

pub struct Tracer {
    config: Config,

    write_csv: Option<WriteCsv>,
    write_mongo: Option<WriteMongo>,
    write_pg: Option<WritePg>,

    send_tx: Option<Sender<SendCmd>>,
    send_rx: Receiver<SendCmd>,
    recv_tx: Sender<RecvCmd>,
    recv_rx: Option<Receiver<RecvCmd>>,
}

impl Tracer {
    pub fn new(config: Config) -> Self {
        let (send_tx, send_rx) = mpsc::channel(1);
        let (recv_tx, recv_rx) = mpsc::channel(1);
        let write_csv = if config.use_csv {
            Some(WriteCsv::new(config.cvs_file_path.clone()))
        } else {
            None
        };
        let write_mongo = if config.use_mongo {
            Some(WriteMongo::new(config.mongo_url.clone()))
        } else {
            None
        };
        let write_pg = if config.use_pg {
            Some(WritePg::new(config.pg_config.clone()))
        } else {
            None
        };
        Self {
            config,
            send_tx: Some(send_tx),
            send_rx,
            recv_tx,
            recv_rx: Some(recv_rx),
            write_csv,
            write_mongo,
            write_pg,
        }
    }
}

impl Trace for Tracer {
    async fn run(&mut self) -> Result<()> {
        let mut c_tx = None;
        let mut c_rx = None;
        if self.write_csv.is_some() {
            let mut write_csv = self.write_csv.take().unwrap();
            c_tx = Some(write_csv.send());
            c_rx = Some(write_csv.recv());
            tokio::spawn(async move { write_csv.run().await });
        }

        let mut m_tx = None;
        let mut m_rx = None;
        if self.write_mongo.is_some() {
            let mut write_mongo = self.write_mongo.take().unwrap();
            m_tx = Some(write_mongo.send());
            m_rx = Some(write_mongo.recv());
            tokio::spawn(async move { write_mongo.run().await });
        }

        let mut p_tx = None;
        let mut p_rx = None;
        if self.write_pg.is_some() {
            let mut write_pg = self.write_pg.take().unwrap();
            p_tx = Some(write_pg.send());
            p_rx = Some(write_pg.recv());
            tokio::spawn(async move { write_pg.run().await });
        }

        let batch_size = self.config.batch_size;
        let duration = std::time::Duration::from_secs(self.config.batch_interval_sec.into());

        let mut batch = Vec::new();
        let mut start_time = std::time::Instant::now();

        loop {
            match self.send_rx.try_recv() {
                Result::Ok(SendCmd::Record(block)) => {
                    batch.push(block);
                    if batch.len() >= batch_size || start_time.elapsed() >= duration {
                        let batch2 = Arc::new(batch.clone());
                        if let Some(c_tx) = c_tx.as_mut() {
                            c_tx.send(WriteSendCmd::RecordBatch(batch2.clone())).await?;
                        }
                        if let Some(m_tx) = m_tx.as_mut() {
                            m_tx.send(WriteSendCmd::RecordBatch(batch2.clone())).await?;
                        }
                        if let Some(p_tx) = p_tx.as_mut() {
                            p_tx.send(WriteSendCmd::RecordBatch(batch2.clone())).await?;
                        }
                        batch.clear();
                        start_time = std::time::Instant::now();
                    }
                }
                Result::Ok(SendCmd::Close) => {
                    if let Some(c_tx) = c_tx.as_mut() {
                        c_tx.send(WriteSendCmd::Close).await?;
                    }
                    if let Some(m_tx) = m_tx.as_mut() {
                        m_tx.send(WriteSendCmd::Close).await?;
                    }
                    if let Some(p_tx) = p_tx.as_mut() {
                        p_tx.send(WriteSendCmd::Close).await?;
                    }
                    break;
                }
                Err(_) => { /* go on */ }
            }

            if let Some(c_rx) = c_rx.as_mut() {
                match c_rx.try_recv() {
                    Result::Ok(WriteRecvCmd::Err(e)) => {
                        self.recv_tx.send(RecvCmd::Err(e)).await?;
                    }
                    _ => { /* go on */ }
                }
            }

            if let Some(m_rx) = m_rx.as_mut() {
                match m_rx.try_recv() {
                    Result::Ok(WriteRecvCmd::Err(e)) => {
                        self.recv_tx.send(RecvCmd::Err(e)).await?;
                    }
                    _ => { /* go on */ }
                }
            }

            if let Some(p_rx) = p_rx.as_mut() {
                match p_rx.try_recv() {
                    Result::Ok(WriteRecvCmd::Err(e)) => {
                        self.recv_tx.send(RecvCmd::Err(e)).await?;
                    }
                    _ => { /* go on */ }
                }
            }
        }

        Ok(())
    }

    fn get_commander(&mut self) -> (Sender<SendCmd>, Receiver<RecvCmd>) {
        (self.send_tx.take().unwrap(), self.recv_rx.take().unwrap())
    }

    fn init(&mut self) -> Result<()> {
        if self.write_csv.is_some() {
            self.write_csv.as_mut().unwrap().init()?;
        }
        if self.write_mongo.is_some() {
            self.write_mongo.as_mut().unwrap().init()?;
        }
        if self.write_pg.is_some() {
            self.write_pg.as_mut().unwrap().init()?;
        }
        Ok(())
    }
}
