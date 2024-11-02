use crate::{
    cluster::{self, ClusterTrait, Config as ClusterConfig},
    config::Config,
    connect::{self, Config as UnionConnectorConfig, Connect, UnionConnector},
    load_balance::{self, LoadBalance, ALGO},
    trace::{self, Trace},
    transmit::{self, Config as OrTransmitterConfig, EventCmd, Hook, OrTransmitter, Transmit},
};
use anyhow::{Ok, Result};
use log::error;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use tokio::sync::{
    broadcast,
    mpsc::{Receiver, Sender},
    oneshot::{self},
};

pub struct App {
    connector: Option<UnionConnector<Message>>,
    transmitter: Option<OrTransmitter<Message>>,

    tracer: Option<EventTracer>,
}

impl App {
    pub fn new(config: Config) -> Self {
        let connector = UnionConnector::new(UnionConnectorConfig {
            use_tcp: config.connector_config.use_tcp,
            tcp_bind_addr: config.connector_config.tcp_bind_addr,
            use_udp: config.connector_config.use_udp,
            udp_bind_addr: config.connector_config.udp_bind_addr,
            msg_buf_size: config.connector_config.msg_buf_size,
        });
        let mut transmitter = OrTransmitter::new(OrTransmitterConfig {
            use_memory: config.transmitter_config.use_memory,
            use_redis: config.transmitter_config.use_redis,
            redis_uri: config.transmitter_config.redis_uri,
        });
        if config.cluster_config.use_cluster {
            let cluster = cluster::Cluster::<Message>::new(ClusterConfig {
                udp_port: config.cluster_config.udp_port,
                etcd_addr: config.cluster_config.etcd_addr,
            });
            let hooker = ClusterHooker::new(cluster);
            transmitter.register_hooker(Box::new(hooker));
        }

        let tracer = if config.tracer_config.use_tracer {
            let core = trace::Tracer::new(trace::Config {
                use_csv: config.tracer_config.use_csv,
                cvs_file_path: config.tracer_config.cvs_file_path,
                use_mongo: config.tracer_config.use_mongo,
                mongo_url: config.tracer_config.mongo_url,
                use_pg: config.tracer_config.use_pg,
                pg_config: config.tracer_config.pg_config,
                batch_interval_sec: config.tracer_config.batch_interval_sec,
                batch_size: config.tracer_config.batch_size,
            });
            Some(EventTracer::new(core))
        } else {
            None
        };
        Self {
            connector: Some(connector),
            transmitter: Some(transmitter),
            tracer,
        }
    }

    pub async fn run(&mut self, mut closed: oneshot::Receiver<()>) -> Result<()> {
        let (close_tx, close_rx) = broadcast::channel(1);

        let mut connector = self.connector.take().unwrap();
        connector.init()?;
        let (c_tx, mut c_rx) = connector.get_commander();
        let close_rx_clone = close_rx.resubscribe();
        tokio::spawn(async move { connector.run(close_rx_clone).await });

        let mut transmitter = self.transmitter.take().unwrap();
        transmitter.init()?;
        let (t_tx, mut t_rx) = transmitter.get_commander();
        let broadcast_rx = transmitter.get_event_recver();
        let close_rx_clone = close_rx.resubscribe();
        tokio::spawn(async move { transmitter.run(close_rx_clone).await });

        if self.tracer.is_some() {
            let mut tracer = self.tracer.take().unwrap();
            tracer.init(broadcast_rx.resubscribe())?;
            let close_rx_clone = close_rx.resubscribe();
            tokio::spawn(async move { tracer.run(close_rx_clone).await });
        }

        loop {
            // msg from connector.
            use connect::RecvCmd as C_RecvCmd;
            use transmit::SendCmd as T_SendCmd;
            match c_rx.try_recv().unwrap() {
                C_RecvCmd::NewMsg(msg) => {
                    t_tx.send(T_SendCmd::InputMsg(msg)).await?;
                }
                C_RecvCmd::NewClient(client) => {
                    t_tx.send(T_SendCmd::NewClient(transmit::ClientInfo::from(client)))
                        .await?;
                }
                C_RecvCmd::DelClient(client) => {
                    t_tx.send(T_SendCmd::DelClient(transmit::ClientInfo::from(client)))
                        .await?;
                }
                C_RecvCmd::Err(e) => {
                    error!("connector error: {}", e);
                }
            }

            // msg from transmitter.
            use connect::SendCmd as C_SendCmd;
            use transmit::RecvCmd as T_RecvCmd;
            match t_rx.try_recv().unwrap() {
                T_RecvCmd::OutputMsg(msg) => {
                    c_tx.send(C_SendCmd::NewMsg(msg)).await?;
                }
                T_RecvCmd::Err(e) => {
                    error!("transmitter error: {}", e);
                }
            }

            // check close signal
            match closed.try_recv() {
                Result::Ok(_) => {
                    if let Err(e) = close_tx.send(()) {
                        error!("broadcast error: {}", e);
                    };
                    break;
                }
                Err(_) => { /* go on */ }
            }
        }

        Ok(())
    }
}

// ==== Message ====

/// Message defines full fields of message in this system.
#[derive(Clone, Debug, Deserialize, Serialize, Default)]
struct Message {
    /// id must be unique across lifetime of this system.
    pub id: String,
    /// request_id must be unique across lifetime of this system.
    pub request_id: String,

    pub created_at: u64,

    pub from_service_name: String,
    pub to_service_name: String,
    pub from_service_no: String,
    pub to_service_no: Option<String>,

    /// load balance algorithm
    pub lb_algo: String,

    pub is_request: bool,
    pub write_op: bool,
    pub success: Option<bool>,
    pub description: Option<String>,

    pub data: Vec<u8>,

    /* Extra fields */
    extra: MessageExtra,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct MessageExtra {
    pub created_at: SystemTime,

    /// which connector to use.
    pub use_connector_name: String,

    /// Check this to_service is local or not.
    pub local_connector: bool,

    pub from_ip_addr: String,
}

impl TryFrom<String> for Message {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        serde_json::from_str(&value).map_err(anyhow::Error::from)
    }
}

impl Into<String> for Message {
    fn into(self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}

impl TryFrom<Vec<u8>> for Message {
    type Error = anyhow::Error;

    fn try_from(value: Vec<u8>) -> std::result::Result<Self, Self::Error> {
        serde_json::from_slice(value.as_slice()).map_err(anyhow::Error::from)
    }
}

impl Into<Vec<u8>> for Message {
    fn into(self) -> Vec<u8> {
        serde_json::to_vec(&self).unwrap()
    }
}

impl connect::MessageTrait for Message {
    fn get_to_service_name(&self) -> String {
        self.to_service_name.clone()
    }

    fn after_created(&mut self) {
        self.extra.created_at = std::time::SystemTime::now();
    }

    fn set_connector_name(&mut self, name: String) {
        self.extra.use_connector_name = name;
    }
}

impl transmit::MessageTrait for Message {
    fn get_id(&self) -> String {
        self.id.clone()
    }

    fn get_to_service_name(&self) -> String {
        self.to_service_name.clone()
    }
}

impl Default for MessageExtra {
    fn default() -> Self {
        Self {
            created_at: SystemTime::now(),
            use_connector_name: String::new(),
            local_connector: false,
            from_ip_addr: String::new(),
        }
    }
}

// ==== ClientInfo ====

impl From<connect::ClientInfo> for transmit::ClientInfo {
    fn from(value: connect::ClientInfo) -> Self {
        Self {
            service_name: value.service_name,
            service_no: value.service_no,
            weight: value.weight,
        }
    }
}

// ==== Hook Cluster ====

impl cluster::MessageTrait for Message {
    fn get_to_service_name(&self) -> String {
        self.to_service_name.clone()
    }

    fn set_new_to_service_name(&mut self, service_name: String) {
        self.to_service_name = service_name
    }
}

struct ClusterHooker {
    inner: Option<cluster::Cluster<Message>>,

    tx: Option<Sender<cluster::SendCmd<Message>>>,
    rx: Option<Receiver<cluster::RecvCmd<Message>>>,

    init: bool,
}

impl ClusterHooker {
    fn new(cluster: cluster::Cluster<Message>) -> Self {
        Self {
            inner: Some(cluster),
            tx: None,
            rx: None,
            init: false,
        }
    }
}

impl Hook<Message> for ClusterHooker {
    fn name(&self) -> &'static str {
        "cluster"
    }

    fn nonblocking_run(&mut self) -> Result<()> {
        if !self.init {
            let mut inner = self.inner.take().unwrap();
            let (tx, rx) = inner.get_commander();
            inner.init()?;
            self.tx = Some(tx);
            self.rx = Some(rx);
            self.init = true;

            tokio::spawn(async move { inner.run().await });
        }

        Ok(())
    }

    fn no_connector(&mut self, msg: Message) -> Result<()> {
        self.tx
            .as_mut()
            .unwrap()
            .blocking_send(cluster::SendCmd::TransferMsg(msg))
            .map_err(anyhow::Error::from)
    }

    fn close(&mut self) {
        let _ = self.tx.as_mut().unwrap().send(cluster::SendCmd::Close);
    }
}

// ==== Hook LoadBalance ====

pub struct LoadBalanceHooker {
    inner: Option<load_balance::LoadBalancer>,

    init: bool,
}

impl Hook<Message> for LoadBalanceHooker {
    fn name(&self) -> &'static str {
        "load_balance"
    }

    fn nonblocking_run(&mut self) -> Result<()> {
        if !self.init {
            self.inner.as_mut().unwrap().init()?;
            self.init = true;
        }

        self.inner.as_mut().unwrap().nonblocking_run()?;
        Ok(())
    }
    fn before_exchange(
        &mut self,
        msg: std::borrow::Cow<Message>,
    ) -> Result<transmit::HookReply<Message>> {
        if msg.to_service_no.is_some() {
            return Ok(transmit::HookReply::Continue);
        }

        let algo;
        match ALGO::try_from(msg.lb_algo.clone()) {
            Result::Ok(algo0) => {
                algo = algo0;
            }
            Err(_) => return Err(anyhow::Error::msg("invalid lb algo")),
        }
        let res = self
            .inner
            .as_mut()
            .unwrap()
            .get_service(load_balance::Request {
                msg_use_algo: algo,
                service_name: msg.to_service_name.clone(),
                addr: msg.extra.from_ip_addr.to_string(),
                addr_hash_aux: "".to_string(), // todo: ???
            })
            .unwrap();
        let mut msg = msg.into_owned();
        msg.to_service_name = res.service_name;
        msg.to_service_no = Some(res.service_number);
        Ok(transmit::HookReply::Modified(msg))
    }

    fn new_client(&mut self, _client: transmit::ClientInfo) {}

    fn close(&mut self) {}
}

// ==== EventRecver Trait ====

trait EventRecver {
    fn init(&mut self, event_recver: broadcast::Receiver<EventCmd<Message>>) -> Result<()>;
    async fn run(&mut self, closed: broadcast::Receiver<()>) -> Result<()>;
}

// ==== Event Trace ====

impl Into<trace::TraceBlock> for Message {
    fn into(self) -> trace::TraceBlock {
        trace::TraceBlock {
            msg_id: self.id,
            to_service_name: self.to_service_name,
            to_service_no: self.to_service_no.unwrap(),
            request_id: self.request_id,
            created_at: self.extra.created_at,
            from_service_name: self.from_service_name,
            from_service_no: self.from_service_no,
            is_request: self.is_request,
            write_op: self.write_op,
            success: self.success,
            description: self.description,
        }
    }
}

struct EventTracer {
    core: Option<trace::Tracer>,
    event_recver: Option<broadcast::Receiver<EventCmd<Message>>>,
}

impl EventTracer {
    fn new(core: trace::Tracer) -> Self {
        Self {
            event_recver: None,
            core: Some(core),
        }
    }
}

impl EventRecver for EventTracer {
    fn init(&mut self, event_recver: broadcast::Receiver<EventCmd<Message>>) -> Result<()> {
        self.core.as_mut().unwrap().init()?;
        self.event_recver = Some(event_recver);
        Ok(())
    }

    async fn run(&mut self, mut closed: broadcast::Receiver<()>) -> Result<()> {
        let mut core = self.core.take().unwrap();
        let (tx, mut rx) = core.get_commander();
        tokio::spawn(async move { core.run().await });

        let mut recv = self.event_recver.take().unwrap();

        loop {
            match recv.try_recv() {
                Result::Ok(EventCmd::NewMsg(msg)) => {
                    tx.send(trace::SendCmd::Record(msg.into())).await?;
                }
                _ => { /* go on */ }
            }

            match rx.try_recv() {
                Result::Ok(trace::RecvCmd::Err(e)) => {
                    return Err(anyhow::Error::from(e));
                }
                _ => { /* go on */ }
            }

            match closed.try_recv() {
                Result::Ok(_) => {
                    tx.send(trace::SendCmd::Close).await?;
                    break;
                }
                _ => { /* go on */ }
            }
        }

        Ok(())
    }
}
