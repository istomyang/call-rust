use anyhow::{Error, Ok, Result};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::future::Future;
use tokio::sync::{
    broadcast,
    mpsc::{self, Receiver, Sender},
};

pub mod core_tcp;
pub mod core_udp;

use core_tcp as tcp;
use core_udp as udp;

// ==== Trait Connect ====

pub trait Connect<M>
where
    M: MessageTrait,
{
    fn init(&mut self) -> Result<()>;
    fn run(&mut self, closed: broadcast::Receiver<()>) -> impl Future<Output = Result<()>> + Send;

    fn get_commander(&mut self) -> (Sender<SendCmd<M>>, Receiver<RecvCmd<M>>);
}

pub trait MessageTrait:
    Sync + Send + Clone + Default + Into<Vec<u8>> + TryFrom<Vec<u8>, Error = anyhow::Error>
{
    fn get_to_service_name(&self) -> String;
    fn after_created(&mut self);
    fn set_connector_name(&mut self, name: String);
}

pub enum SendCmd<M>
where
    M: MessageTrait,
{
    NewMsg(M),
}

pub enum RecvCmd<M>
where
    M: MessageTrait,
{
    NewMsg(M),
    NewClient(ClientInfo),
    DelClient(ClientInfo),
    Err(Error),
}

#[derive(Clone)]
pub struct ClientInfo {
    pub service_name: String,
    pub service_no: String,
    pub client_id: String,
    pub weight: u8,
}

#[derive(Serialize, Deserialize)]
pub struct ClientRegister {
    pub service_name: String,
    pub service_no: String,
    pub weight: u8,
}

impl TryFrom<Vec<u8>> for ClientRegister {
    type Error = Error;

    fn try_from(value: Vec<u8>) -> std::result::Result<Self, Self::Error> {
        Ok(serde_json::from_slice(value.as_slice())?)
    }
}

// ==== Trait Core ====

pub trait Core {
    fn init(&mut self) -> Result<()>;
    fn run(&mut self) -> impl Future<Output = Result<()>> + Send;

    fn get_commander(&mut self) -> (Sender<CoreSendCmd>, Receiver<CoreRecvCmd>);
}

pub type ClientID = String;
pub type RawMessage = Vec<u8>;

pub enum CoreSendCmd {
    NewMsg(ClientID, RawMessage),
    Close,
}

pub enum CoreRecvCmd {
    NewMsg(ClientID, RawMessage),
    Err(ClientID, String),
}

// ==== Impl Connector ====

pub struct Connector<T, M>
where
    T: Core,
    M: MessageTrait,
{
    core: Option<T>,
    core_name: &'static str,

    send_rx: Receiver<SendCmd<M>>,
    recv_tx: Sender<RecvCmd<M>>,

    send_tx: Option<Sender<SendCmd<M>>>,
    recv_rx: Option<Receiver<RecvCmd<M>>>,

    clients: DashMap<String, ClientInfo>, // service_name => client_info
    clients2: DashMap<String, String>,    // client_id => service_name
}

impl<T, M> Connector<T, M>
where
    T: Core,
    M: MessageTrait,
{
    pub fn new(name: &'static str, core: T) -> Self {
        let (send_tx, send_rx) = mpsc::channel(1024);
        let (recv_tx, recv_rx) = mpsc::channel(1024);
        Self {
            core: Some(core),
            core_name: name,
            send_tx: Some(send_tx),
            send_rx,
            recv_tx,
            recv_rx: Some(recv_rx),
            clients: DashMap::new(),
            clients2: DashMap::new(),
        }
    }

    fn get_recv_tx(&mut self) -> Sender<RecvCmd<M>> {
        self.recv_tx.clone()
    }
}

impl<T, M> Connect<M> for Connector<T, M>
where
    T: Core + Send + 'static,
    M: MessageTrait + 'static,
{
    fn init(&mut self) -> Result<()> {
        self.core.as_mut().unwrap().init()?;
        Ok(())
    }

    async fn run(&mut self, mut closed: broadcast::Receiver<()>) -> Result<()> {
        let mut core = self.core.take().unwrap();
        let (core_sender, mut core_recver) = core.get_commander();
        tokio::spawn(async move { core.run().await });

        loop {
            // msg from core.
            match core_recver.try_recv() {
                Result::Ok(CoreRecvCmd::NewMsg(client_id, msg)) => {
                    // check new client
                    if !self.clients2.contains_key(&client_id) {
                        match ClientRegister::try_from(msg.clone()) {
                            Result::Ok(reg) => {
                                let client_info = ClientInfo {
                                    service_name: reg.service_name.clone(),
                                    service_no: reg.service_no,
                                    client_id: client_id.clone(),
                                    weight: reg.weight,
                                };
                                let client_info_clone = client_info.clone();

                                // save client
                                self.clients.insert(reg.service_name.clone(), client_info);
                                self.clients2.insert(client_id, reg.service_name.clone());

                                // send new client

                                self.get_recv_tx()
                                    .send(RecvCmd::NewClient(client_info_clone))
                                    .await
                                    .map_err(Error::from)?;
                            }
                            Result::Err(_) => {
                                self.get_recv_tx()
                                    .send(RecvCmd::Err(Error::msg(format!(
                                        "client register error: {}, {}",
                                        client_id,
                                        String::from_utf8_lossy(msg.as_slice())
                                    ))))
                                    .await
                                    .map_err(Error::from)?;
                            }
                        };
                    }

                    // send msg
                    match M::try_from(msg.clone()) {
                        Result::Ok(mut msg) => {
                            msg.after_created();
                            msg.set_connector_name(self.core_name.to_string());
                            self.get_recv_tx()
                                .send(RecvCmd::NewMsg(msg))
                                .await
                                .map_err(Error::from)?;
                        }
                        Result::Err(e) => {
                            self.recv_tx
                                .clone()
                                .send(RecvCmd::Err(e))
                                .await
                                .map_err(Error::from)?;
                        }
                    }
                }
                Result::Ok(CoreRecvCmd::Err(client_id, _err)) => {
                    // remove client
                    let service_name = self.clients2.remove(&client_id).unwrap().1;
                    let del = self.clients.remove(&service_name).unwrap().1;

                    self.get_recv_tx()
                        .send(RecvCmd::DelClient(del))
                        .await
                        .map_err(Error::from)?;
                }
                Err(_) => { /* go on */ }
            }

            // msg from upper
            match self.send_rx.try_recv() {
                Result::Ok(cmd) => {
                    match cmd {
                        SendCmd::NewMsg(msg) => {
                            // check has service name
                            if self.clients.contains_key(&msg.get_to_service_name()) {
                                // send msg to core
                                let client_id = self
                                    .clients
                                    .get(&msg.get_to_service_name())
                                    .unwrap()
                                    .value()
                                    .client_id
                                    .clone();
                                core_sender
                                    .send(CoreSendCmd::NewMsg(client_id, msg.into()))
                                    .await
                                    .map_err(Error::from)?;
                            }
                        }
                    }
                }
                Err(_) => { /* go on */ }
            }

            // closed
            match closed.try_recv() {
                Result::Ok(_) => {
                    core_sender.send(CoreSendCmd::Close).await?;
                    break;
                }
                Err(_) => { /* go on */ }
            }
        }

        Ok(())
    }

    fn get_commander(&mut self) -> (Sender<SendCmd<M>>, Receiver<RecvCmd<M>>) {
        (self.send_tx.take().unwrap(), self.recv_rx.take().unwrap())
    }
}

// ==== Constructor Utility ====

#[allow(dead_code)]
pub enum CoreConfig {
    Udp(udp::Config),
    Tcp(tcp::Config),
}

#[allow(dead_code)]
pub enum Connectors<M>
where
    M: MessageTrait,
{
    Udp(Connector<udp::UdpCore, M>),
    Tcp(Connector<tcp::TcpCore, M>),
}

#[allow(dead_code)]
pub fn with<M>(core_config: CoreConfig) -> Connectors<M>
where
    M: MessageTrait,
{
    match core_config {
        CoreConfig::Udp(config) => {
            Connectors::Udp(Connector::new("udp", udp::UdpCore::new(config)))
        }
        CoreConfig::Tcp(config) => {
            Connectors::Tcp(Connector::new("udp", tcp::TcpCore::new(config)))
        }
    }
}

pub fn with_udp<M>(config: udp::Config) -> Connector<udp::UdpCore, M>
where
    M: MessageTrait,
{
    Connector::new("udp", udp::UdpCore::new(config))
}

pub fn with_tcp<M>(config: tcp::Config) -> Connector<tcp::TcpCore, M>
where
    M: MessageTrait,
{
    Connector::new("tcp", tcp::TcpCore::new(config))
}

// ==== Union Connector ====

/// UnionConnector holds multiple connectors at running time.
pub struct UnionConnector<M>
where
    M: MessageTrait,
{
    connector_tcp: Option<Connector<tcp::TcpCore, M>>,
    connector_udp: Option<Connector<udp::UdpCore, M>>,

    send_tx: Option<Sender<SendCmd<M>>>,
    send_rx: Option<Receiver<SendCmd<M>>>,

    recv_tx: Option<Sender<RecvCmd<M>>>,
    recv_rx: Option<Receiver<RecvCmd<M>>>,
}

pub struct Config {
    pub use_tcp: bool,
    pub tcp_bind_addr: String,
    pub use_udp: bool,
    pub udp_bind_addr: String,
    pub msg_buf_size: usize,
}

impl<M> UnionConnector<M>
where
    M: MessageTrait,
{
    pub fn new(config: Config) -> Self {
        let tcp = if config.use_tcp {
            Some(with_tcp(tcp::Config {
                bind_addr: config.tcp_bind_addr,
                buf_size: config.msg_buf_size,
            }))
        } else {
            None
        };
        let udp = if config.use_udp {
            Some(with_udp(udp::Config {
                bind_addr: config.udp_bind_addr,
                buf_size: config.msg_buf_size,
            }))
        } else {
            None
        };
        let (tx1, rx1) = mpsc::channel(32);
        let (tx2, rx2) = mpsc::channel(32);
        Self {
            connector_tcp: tcp,
            connector_udp: udp,
            send_tx: Some(tx1),
            send_rx: Some(rx1),
            recv_tx: Some(tx2),
            recv_rx: Some(rx2),
        }
    }
}

impl<M> Connect<M> for UnionConnector<M>
where
    M: MessageTrait + 'static,
{
    fn init(&mut self) -> Result<()> {
        if self.connector_tcp.is_some() {
            self.connector_tcp.as_mut().unwrap().init()?;
        }
        if self.connector_udp.is_some() {
            self.connector_udp.as_mut().unwrap().init()?;
        }

        Ok(())
    }

    async fn run(&mut self, mut closed: broadcast::Receiver<()>) -> Result<()> {
        let tcp_tx;
        let mut tcp_rx;
        {
            let mut tcp = self.connector_tcp.take().unwrap();
            let (tx, rx) = tcp.get_commander();
            tcp_tx = Some(tx);
            tcp_rx = Some(rx);
            let close_rx = closed.resubscribe();
            tokio::spawn(async move { tcp.run(close_rx).await });
        }

        let udp_tx;
        let mut udp_rx;
        {
            let mut udp = self.connector_udp.take().unwrap();
            let (tx, rx) = udp.get_commander();
            udp_tx = Some(tx);
            udp_rx = Some(rx);
            let close_rx = closed.resubscribe();
            tokio::spawn(async move { udp.run(close_rx).await });
        }

        loop {
            let upper_tx = self.recv_tx.as_mut().unwrap();
            let upper_rx = self.send_rx.as_mut().unwrap();

            // msg from tcp core, send to upper.
            if let Some(rx) = tcp_rx.as_mut() {
                if let Result::Ok(cmd) = rx.try_recv() {
                    upper_tx.send(cmd).await?;
                }
            }

            // msg from udp core, send to upper.
            if let Some(rx) = udp_rx.as_mut() {
                if let Result::Ok(cmd) = rx.try_recv() {
                    upper_tx.send(cmd).await?;
                }
            }

            // msg from upper, find and send to core.
            match upper_rx.try_recv().unwrap() {
                SendCmd::NewMsg(msg) => {
                    let msg_clone = msg.clone();
                    if let Some(tx) = tcp_tx.as_ref() {
                        tx.send(SendCmd::NewMsg(msg)).await?;
                    }
                    if let Some(tx) = udp_tx.as_ref() {
                        tx.send(SendCmd::NewMsg(msg_clone)).await?;
                    }
                }
            }

            // close
            if closed.try_recv().is_ok() {
                break;
            }
        }

        Ok(())
    }

    fn get_commander(&mut self) -> (Sender<SendCmd<M>>, Receiver<RecvCmd<M>>) {
        (self.send_tx.take().unwrap(), self.recv_rx.take().unwrap())
    }
}
