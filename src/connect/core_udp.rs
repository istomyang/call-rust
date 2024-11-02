use super::{Core, CoreRecvCmd, CoreSendCmd};
use anyhow::{Ok, Result};

use bytes::BytesMut;
use tokio::{
    io::Interest,
    net::UdpSocket,
    sync::mpsc::{self, channel, Receiver, Sender},
};

pub struct UdpCore {
    config: Config,

    send_tx: Option<Sender<CoreSendCmd>>,
    send_rx: Receiver<CoreSendCmd>,

    recv_tx: Sender<CoreRecvCmd>,
    recv_rx: Option<Receiver<CoreRecvCmd>>,
}

pub struct Config {
    pub bind_addr: String,
    pub buf_size: usize,
}

impl UdpCore {
    pub fn new(config: Config) -> Self {
        let (send_tx, send_rx) = channel(1024);
        let (recv_tx, recv_rx) = channel(1024);
        Self {
            config,
            send_tx: Some(send_tx),
            send_rx,
            recv_tx,
            recv_rx: Some(recv_rx),
        }
    }
}

impl Core for UdpCore {
    fn get_commander(&mut self) -> (Sender<CoreSendCmd>, Receiver<CoreRecvCmd>) {
        (self.send_tx.take().unwrap(), self.recv_rx.take().unwrap())
    }

    fn init(&mut self) -> Result<()> {
        Ok(())
    }

    async fn run(&mut self) -> Result<()> {
        let sock = UdpSocket::bind(self.config.bind_addr.as_str()).await?;
        let mut buf = BytesMut::with_capacity(self.config.buf_size);

        let (tx, mut rx) = mpsc::channel(1024);

        loop {
            // handle msg from remote node.
            match sock.try_recv_from(&mut buf) {
                Result::Ok((len, src)) => {
                    let addr = src.to_string();
                    let msg: Vec<u8> = (&buf[..len]).into();
                    self.recv_tx.send(CoreRecvCmd::NewMsg(addr, msg)).await?;
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => { /* go on */ }
                Err(e) => {
                    return Err(e.into());
                }
            }

            // handle msg from upper.
            match self.send_rx.try_recv() {
                Result::Ok(CoreSendCmd::NewMsg(addr, msg)) => {
                    tx.send((addr, msg)).await?;
                }
                Result::Ok(CoreSendCmd::Close) => {
                    break;
                }
                _ => { /* go on */ }
            }

            // handle rx
            let ready = sock.ready(Interest::WRITABLE).await?;
            if ready.is_writable() {
                match rx.recv().await {
                    Some((addr, msg)) => {
                        sock.send_to(msg.as_slice(), addr.as_str()).await?;
                    }
                    None => { /* go on */ }
                }
            }
        }

        Ok(())
    }
}
