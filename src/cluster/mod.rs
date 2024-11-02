use std::{fmt, future::Future};

use anyhow::{Error, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{self, Receiver, Sender};

pub mod net_udp;
pub mod store_etcd;

// ===== Cluster Trait =====

pub trait ClusterTrait<M>
where
    M: MessageTrait,
{
    fn init(&mut self) -> Result<()>;
    fn run(&mut self) -> impl Future<Output = Result<()>> + Send;
    fn get_commander(&mut self) -> (Sender<SendCmd<M>>, Receiver<RecvCmd<M>>);
}

pub enum SendCmd<M: MessageTrait> {
    Register(NodeInfo),
    TransferMsg(M),
    Close,
}

pub enum RecvCmd<M: MessageTrait> {
    NewMsg(M),
    Err(Error),
}

// ==== Net Trait ====

pub(in crate::cluster) trait Net {
    fn init(&mut self) -> Result<()>;
    fn run(&mut self) -> impl Future<Output = Result<()>> + Send;
    fn get_commander(&mut self) -> (Sender<NetSendCmd>, Receiver<NetRecvCmd>);
}

pub(in crate::cluster) type ClientID = String;
pub(in crate::cluster) type RawMessage = String;

fn from_client_id(ip: &str, port: u32) -> ClientID {
    format!("{}:{}", ip, port)
}

pub(in crate::cluster) enum NetSendCmd {
    NewMsg(ClientID, RawMessage),
    Close,
}

pub(in crate::cluster) enum NetRecvCmd {
    NewMsg(ClientID, RawMessage),
}

// ==== Store Trait ====

/// Store defines register node info into shared store which can be queried by other node.
pub(in crate::cluster) trait StoreTrait {
    fn init(&mut self) -> Result<()>;
    fn run(&mut self) -> impl Future<Output = Result<()>> + Send;
    fn get_commander(&mut self) -> (Sender<StoreSendCmd>, Receiver<StoreRecvCmd>);
}

pub(in crate::cluster) enum StoreSendCmd {
    RegisterNode(NodeInfo),
    FindNodes(String), // service_name
    Close,
}

pub(in crate::cluster) enum StoreRecvCmd {
    FindNodesResult(Result<Vec<NodeInfo>, Error>),
}

#[derive(Serialize, Deserialize, Clone)]
pub struct NodeInfo {
    pub name: String, // service_name
    pub ip: String,
    pub port: u32,
    pub services: Vec<String>, // numbers
}

impl TryFrom<String> for NodeInfo {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        serde_json::from_str(&value).map_err(anyhow::Error::from)
    }
}

impl From<&str> for NodeInfo {
    fn from(value: &str) -> Self {
        serde_json::from_str(value).unwrap()
    }
}

impl Into<String> for NodeInfo {
    fn into(self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}

pub trait MessageTrait:
    fmt::Debug + Send + Sync + Clone + Into<String> + TryFrom<String, Error = anyhow::Error>
{
    fn get_to_service_name(&self) -> String;
    fn set_new_to_service_name(&mut self, service_name: String);
}

// ===== Cluster Impl =====

use net_udp::NetUdp;
use store_etcd::StoreEtcd;

pub struct Cluster<M>
where
    M: MessageTrait,
{
    net: Option<NetUdp>,
    store: Option<StoreEtcd>,

    send_tx: Option<Sender<SendCmd<M>>>,
    send_rx: Receiver<SendCmd<M>>,

    recv_tx: Sender<RecvCmd<M>>,
    recv_rx: Option<Receiver<RecvCmd<M>>>,
}

pub struct Config {
    pub udp_port: u32,
    pub etcd_addr: String,
}

impl<M> Cluster<M>
where
    M: MessageTrait,
{
    pub fn new(config: Config) -> Self {
        let (send_tx, send_rx) = mpsc::channel(32);
        let (recv_tx, recv_rx) = mpsc::channel(32);
        Self {
            net: Some(NetUdp::new(config.udp_port)),
            store: Some(StoreEtcd::new(config.etcd_addr)),
            send_tx: Some(send_tx),
            send_rx,
            recv_tx,
            recv_rx: Some(recv_rx),
        }
    }
}

impl<M> ClusterTrait<M> for Cluster<M>
where
    M: MessageTrait + 'static,
{
    fn init(&mut self) -> Result<()> {
        self.net.as_mut().unwrap().init()?;
        self.store.as_mut().unwrap().init()?;
        Ok(())
    }

    async fn run(&mut self) -> Result<()> {
        let mut net = self.net.take().unwrap();
        let (n_tx, mut n_rx) = net.get_commander();
        tokio::spawn(async move { net.run().await });

        let mut store = self.store.take().unwrap();
        let (s_tx, mut s_rx) = store.get_commander();
        tokio::spawn(async move { store.run().await });

        loop {
            // msg from super.
            match self.send_rx.try_recv() {
                Result::Ok(cmd) => match cmd {
                    SendCmd::Register(node) => s_tx.send(StoreSendCmd::RegisterNode(node)).await?,
                    SendCmd::TransferMsg(msg) => {
                        // find node
                        s_tx.send(StoreSendCmd::FindNodes(msg.get_to_service_name()))
                            .await?;

                        // blocking wait result.
                        match s_rx.blocking_recv().unwrap() {
                            StoreRecvCmd::FindNodesResult(result) => match result {
                                Result::Ok(nodes) => {
                                    let node = nodes.first().unwrap();
                                    n_tx.send(NetSendCmd::NewMsg(
                                        from_client_id(node.ip.as_str(), node.port),
                                        msg.into(),
                                    ))
                                    .await?;
                                }
                                Err(e) => {
                                    let e = Error::msg(format!("find node error: {}", e));
                                    self.recv_tx.send(RecvCmd::Err(e)).await?;
                                }
                            },
                        }
                    }
                    SendCmd::Close => {
                        n_tx.send(NetSendCmd::Close).await?;
                        s_tx.send(StoreSendCmd::Close).await?;
                        break;
                    }
                },
                Err(_) => { /* go on */ }
            }

            // msg from remote
            match n_rx.try_recv() {
                Result::Ok(cmd) => match cmd {
                    NetRecvCmd::NewMsg(client, msg) => match M::try_from(msg.clone()) {
                        Result::Ok(msg) => {
                            self.recv_tx.send(RecvCmd::NewMsg(msg)).await?;
                        }
                        Err(_) => {
                            let e = Error::msg(format!("msg parse error from {}: {}", client, msg));
                            self.recv_tx.send(RecvCmd::Err(e)).await?;
                        }
                    },
                },
                Err(_) => { /* go on */ }
            }
        }

        Ok(())
    }

    fn get_commander(&mut self) -> (Sender<SendCmd<M>>, Receiver<RecvCmd<M>>) {
        (self.send_tx.take().unwrap(), self.recv_rx.take().unwrap())
    }
}
