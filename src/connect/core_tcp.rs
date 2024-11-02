use std::{
    collections::HashMap,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
};

use super::{Core, CoreRecvCmd, CoreSendCmd};
use anyhow::{Ok, Result};

use bytes::BytesMut;
use tokio::sync::{
    broadcast,
    mpsc::{channel, Receiver, Sender},
};

pub struct TcpCore {
    config: Config,

    send_tx: Option<Sender<CoreSendCmd>>,
    send_rx: Receiver<CoreSendCmd>,

    recv_tx: Sender<CoreRecvCmd>,
    recv_rx: Option<Receiver<CoreRecvCmd>>,

    clients: HashMap<String, Client>, // addr => client

    listener: Option<TcpListener>,
}

pub struct Config {
    pub bind_addr: String,
    pub buf_size: usize,
}

impl TcpCore {
    pub fn new(config: Config) -> Self {
        let (send_tx, send_rx) = channel(1024);
        let (recv_tx, recv_rx) = channel(1024);
        Self {
            config,
            send_tx: Some(send_tx),
            send_rx,
            recv_tx,
            recv_rx: Some(recv_rx),
            clients: HashMap::new(),
            listener: None,
        }
    }

    async fn handle_stream(
        stream: TcpStream,
        send: Sender<CoreRecvCmd>,
        mut recv: Receiver<Vec<u8>>,
        mut close: broadcast::Receiver<()>,
        buf_size: usize,
    ) -> Result<()> {
        let mut stream = stream;
        stream.set_nonblocking(true)?;
        let addr = stream.peer_addr().unwrap().to_string();
        let mut buf = BytesMut::with_capacity(buf_size);

        loop {
            // handle recv from remote: send msg to upper.
            match stream.read(&mut buf) {
                Result::Ok(len) => {
                    let msg = (&buf[..len]).into();
                    send.blocking_send(CoreRecvCmd::NewMsg(addr.clone(), msg))?;
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => { /* go on */ }
                Err(e) => {
                    send.blocking_send(CoreRecvCmd::Err(addr.clone(), e.to_string()))?;
                    break;
                }
            };

            // 2. handle send from upper: send msg to remote.
            match recv.try_recv() {
                Result::Ok(msg) => {
                    stream.write_all(msg.as_slice())?;
                }
                _ => { /* go on */ }
            }

            // 3. listen close
            match close.try_recv() {
                Result::Ok(_) => {
                    stream.shutdown(std::net::Shutdown::Both)?;
                    break;
                }
                _ => {}
            }
        }
        Ok(())
    }
}

impl Core for TcpCore {
    fn get_commander(&mut self) -> (Sender<CoreSendCmd>, Receiver<CoreRecvCmd>) {
        (self.send_tx.take().unwrap(), self.recv_rx.take().unwrap())
    }

    fn init(&mut self) -> Result<()> {
        let listener = TcpListener::bind(self.config.bind_addr.as_str())?;
        listener.set_nonblocking(true)?;
        self.listener = Some(listener);
        Ok(())
    }

    async fn run(&mut self) -> Result<()> {
        let (close_tx, _) = broadcast::channel(1);

        loop {
            let listener = self.listener.as_ref().unwrap();
            let close_recv = close_tx.subscribe();

            // 1. listen connections.
            match listener.incoming().next().unwrap() {
                Result::Ok(s) => {
                    let (t1, r1) = channel(1024);
                    let send = self.recv_tx.clone();
                    let client = Client { send: t1 };
                    let addr = s.peer_addr().unwrap().to_string();
                    self.clients.insert(addr, client);

                    tokio::spawn(Self::handle_stream(
                        s,
                        send,
                        r1,
                        close_recv,
                        self.config.buf_size,
                    ));
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => { /* go on */ }
                Err(e) => panic!("encountered IO error: {e}"),
            }

            // 2. handle msg from upper.
            match self.send_rx.try_recv() {
                Result::Ok(CoreSendCmd::NewMsg(addr, msg)) => {
                    let client = self.clients.get_mut(&addr).unwrap();
                    client.send.blocking_send(msg)?;
                }
                Result::Ok(CoreSendCmd::Close) => {
                    close_tx.send(())?;
                    break;
                }
                _ => {}
            }
        }

        Ok(())
    }
}

struct Client {
    send: Sender<Vec<u8>>,
}
