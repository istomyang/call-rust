use std::sync::{Arc, RwLock};

use super::{Net, NetRecvCmd, NetSendCmd};
use anyhow::{Ok, Result};
use tokio::{
    net::UdpSocket,
    sync::mpsc::{self, Receiver, Sender},
};

pub(in crate::cluster) struct NetUdp {
    port: u32,

    recv_tx: Sender<NetRecvCmd>,
    recv_rx: Option<Receiver<NetRecvCmd>>,

    send_tx: Option<Sender<NetSendCmd>>,
    send_rx: Option<Receiver<NetSendCmd>>,

    closed: Arc<RwLock<bool>>,
}

impl NetUdp {
    pub fn new(port: u32) -> Self {
        let (send_tx, send_rx) = mpsc::channel(32);
        let (recv_tx, recv_rx) = mpsc::channel(32);
        Self {
            port,
            recv_tx,
            recv_rx: Some(recv_rx),
            send_tx: Some(send_tx),
            send_rx: Some(send_rx),
            closed: Arc::new(RwLock::new(false)),
        }
    }
}

impl Net for NetUdp {
    async fn run(&mut self) -> Result<()> {
        let sock = UdpSocket::bind(format!("0.0.0.0:{}", self.port)).await?;
        let sock_arc = Arc::new(sock);

        {
            let sock1 = sock_arc.clone();
            let mut recv = self.send_rx.take().unwrap();
            let closed = self.closed.clone();
            tokio::spawn(async move {
                while let Some(cmd) = recv.recv().await {
                    match cmd {
                        NetSendCmd::NewMsg(addr, msg) => {
                            let a = msg.as_bytes();
                            let _ = sock1.send_to(a, addr.to_string()).await;
                        }
                        NetSendCmd::Close => {
                            *closed.write().unwrap() = true;
                            break;
                        }
                    }
                }
            });
        }

        let mut buf = vec![0u8; 8192];
        let sock2 = sock_arc.clone();
        loop {
            if *self.closed.read().unwrap() {
                break;
            }

            let (len, addr) = sock2.recv_from(&mut buf).await?;
            let msg = String::from_utf8_lossy(&buf[..len]).to_string();
            let addr = addr.to_string();
            let cmd = NetRecvCmd::NewMsg(addr, msg);
            self.recv_tx.clone().send(cmd).await?;
        }

        Ok(())
    }

    fn init(&mut self) -> Result<()> {
        Ok(())
    }

    fn get_commander(&mut self) -> (Sender<NetSendCmd>, Receiver<NetRecvCmd>) {
        (self.send_tx.take().unwrap(), self.recv_rx.take().unwrap())
    }
}
