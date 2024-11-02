use super::{NodeInfo, StoreRecvCmd, StoreSendCmd, StoreTrait};
use anyhow::{Error, Result};
use etcd_client::Client;
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot,
};

pub(in crate::cluster) struct StoreEtcd {
    etcd_addr: String,

    send_tx: Option<Sender<StoreSendCmd>>,
    send_rx: Receiver<StoreSendCmd>,

    recv_tx: Sender<StoreRecvCmd>,
    recv_rx: Option<Receiver<StoreRecvCmd>>,
}

impl StoreEtcd {
    pub fn new(etcd_addr: String) -> Self {
        let (send_tx, send_rx) = mpsc::channel(32);
        let (recv_tx, recv_rx) = mpsc::channel(32);
        Self {
            etcd_addr,
            send_tx: Some(send_tx),
            send_rx,
            recv_tx,
            recv_rx: Some(recv_rx),
        }
    }

    fn key() -> &'static str {
        "call-rust/cluster/nodes"
    }
}

impl StoreTrait for StoreEtcd {
    async fn run(&mut self) -> Result<()> {
        let mut client = Client::connect([self.etcd_addr.as_str()], None).await?;

        loop {
            match self.send_rx.try_recv() {
                Ok(StoreSendCmd::FindNodes(service_name)) => {
                    let mut resp = client.get(service_name.clone(), None).await?;
                    let kvs = resp.take_kvs();
                    let r: Vec<_> = kvs
                        .iter()
                        .map(|kv| NodeInfo::from(kv.value_str().unwrap()))
                        .collect();

                    if r.is_empty() {
                        let e =
                            Error::msg(format!("no nodes for service: {}", service_name.clone()));
                        self.recv_tx
                            .clone()
                            .send(StoreRecvCmd::FindNodesResult(Err(e)))
                            .await?;
                    } else {
                        self.recv_tx
                            .clone()
                            .send(StoreRecvCmd::FindNodesResult(Ok(r)))
                            .await?;
                    }
                }
                Ok(StoreSendCmd::RegisterNode(service_info)) => {
                    let v: String = service_info.into();
                    client.put(Self::key(), v, None).await?;
                }
                Ok(StoreSendCmd::Close) => {
                    break;
                }
                Err(_) => break,
            }
        }

        Ok(())
    }

    fn init(&mut self) -> Result<()> {
        Ok(())
    }

    fn get_commander(&mut self) -> (Sender<StoreSendCmd>, Receiver<StoreRecvCmd>) {
        (self.send_tx.take().unwrap(), self.recv_rx.take().unwrap())
    }
}
