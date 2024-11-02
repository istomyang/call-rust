use anyhow::Result;
use log::info;
use serde::Serialize;
use std::{borrow::Cow, future::Future};
use tokio::sync::{
    broadcast,
    mpsc::{self, Receiver, Sender},
    oneshot,
};

pub mod exchange_memory;
pub mod exchange_redis;

use exchange_memory::MemoryExchange;
use exchange_redis::RedisExchange;

pub trait Transmit<M>
where
    M: MessageTrait,
{
    fn init(&mut self) -> Result<()>;
    fn register_hooker(&mut self, hooker: Box<dyn Hook<M>>);
    fn run(&mut self, closed: broadcast::Receiver<()>) -> impl Future<Output = Result<()>> + Send;

    fn get_commander(&mut self) -> (Sender<SendCmd<M>>, Receiver<RecvCmd<M>>);
    fn get_event_recver(&mut self) -> broadcast::Receiver<EventCmd<M>>;
}

pub enum SendCmd<M: MessageTrait> {
    InputMsg(M),
    NewClient(ClientInfo),
    DelClient(ClientInfo),
}

pub enum RecvCmd<M: MessageTrait> {
    OutputMsg(M),
    Err(anyhow::Error),
}

/// Event is asynchronous.
#[derive(Clone)]
pub enum EventCmd<M: MessageTrait> {
    NewMsg(M),
    NewClient(ClientInfo),
    DelClient(ClientInfo),
}

#[derive(Clone)]
pub struct ClientInfo {
    pub service_name: String,
    pub service_no: String,
    pub weight: u8,
}

/// Exchange plays a role of cutting the peak to fill the valley.
pub trait Exchange {
    fn init(&mut self) -> Result<()>;
    fn nonblocking_run(&mut self) -> Result<()>;
    fn close(&mut self) -> Result<()>;

    fn send(&mut self, data: String) -> Result<()>;
    fn recv(&mut self) -> oneshot::Receiver<String>;
}

/// Design of Hooker.
/// Hook must be non-blocking, for starters, hook callers must get immediate and synchronous response.
/// Using channel can also simulate synchronous call, but hook implementors can't provide these guarantees.
/// I think implementors should run async loop in it's inner run and use channel to communicate by itself.
pub trait Hook<M>: Send
where
    M: MessageTrait,
{
    fn name(&self) -> &'static str;

    /// hooker must be sync, and event is async.
    fn nonblocking_run(&mut self) -> Result<()>;
    fn close(&mut self);

    fn before_exchange(&mut self, _msg: Cow<M>) -> Result<HookReply<M>> {
        Ok(HookReply::Continue)
    }
    fn after_exchange(&mut self, _msg: Cow<M>) {}

    fn no_connector(&mut self, _msg: M) -> Result<()> {
        Ok(())
    }

    fn new_client(&mut self, _client: ClientInfo) {}
    fn del_client(&mut self, _client: ClientInfo) {}
}

pub enum HookReply<M: MessageTrait> {
    Continue,
    Discard,
    Modified(M),
}

pub trait MessageTrait:
    Serialize + Send + Sync + Clone + Into<String> + TryFrom<String, Error = anyhow::Error>
{
    fn get_id(&self) -> String;
    fn get_to_service_name(&self) -> String;
}

// ==== Transmitter Impl ====

pub struct Transmitter<T, M>
where
    T: Exchange,
    M: MessageTrait,
{
    core: T,

    hookers: Vec<Box<dyn Hook<M>>>,

    event_tx: broadcast::Sender<EventCmd<M>>,
    event_rx: Option<broadcast::Receiver<EventCmd<M>>>,

    send_tx: Option<Sender<SendCmd<M>>>,
    send_rx: Option<Receiver<SendCmd<M>>>,

    recv_tx: Sender<RecvCmd<M>>,
    recv_rx: Option<Receiver<RecvCmd<M>>>,

    service_names: Vec<String>,
}

impl<T, M> Transmitter<T, M>
where
    T: Exchange,
    M: MessageTrait,
{
    pub fn new(core: T) -> Self {
        let (event_tx, event_rx) = broadcast::channel(32);
        let (send_tx, send_rx) = mpsc::channel(32);
        let (recv_tx, recv_rx) = mpsc::channel(32);
        Self {
            core,
            hookers: vec![],
            event_tx,
            event_rx: Some(event_rx),
            send_tx: Some(send_tx),
            send_rx: Some(send_rx),
            recv_tx,
            recv_rx: Some(recv_rx),
            service_names: vec![],
        }
    }
}

impl<T, M> Transmit<M> for Transmitter<T, M>
where
    T: Exchange + Send,
    M: MessageTrait + 'static,
{
    fn init(&mut self) -> Result<()> {
        self.core.init()?;
        Ok(())
    }

    fn register_hooker(&mut self, hooker: Box<dyn Hook<M>>) {
        info!("register hooker: {}", hooker.name());
        self.hookers.push(hooker);
    }

    async fn run(&mut self, mut closed: broadcast::Receiver<()>) -> Result<()> {
        loop {
            // nonblocking run components
            self.core.nonblocking_run()?;
            for hooker in self.hookers.iter_mut() {
                hooker.nonblocking_run()?;
            }

            // handle input msg
            match self.send_rx.as_mut().unwrap().try_recv() {
                Result::Ok(SendCmd::InputMsg(mut msg)) => {
                    // check local connector
                    if self.service_names.contains(&msg.get_to_service_name()) {
                        let mut discard_msg = false;

                        // before hooks
                        for hooker in self.hookers.iter_mut() {
                            match hooker.before_exchange(Cow::Borrowed(&msg))? {
                                HookReply::Continue => { /* go on */ }
                                HookReply::Discard => {
                                    discard_msg = true;
                                }
                                HookReply::Modified(new_msg) => {
                                    msg = new_msg;
                                }
                            }
                        }

                        // emit event
                        if !discard_msg {
                            let _ = self.event_tx.send(EventCmd::NewMsg(msg.clone()));
                        }

                        // enter to core
                        if !discard_msg {
                            self.core.send(msg.into())?;
                        }
                    } else {
                        // give hookers
                        for hooker in self.hookers.iter_mut() {
                            hooker.no_connector(msg.clone())?;
                        }
                    }
                }
                Result::Ok(SendCmd::NewClient(client)) => {
                    // run hookers
                    for h in &mut self.hookers {
                        h.new_client(client.clone());
                    }

                    // emit event
                    let _ = self.event_tx.send(EventCmd::NewClient(client.clone()));

                    // add service name
                    self.service_names.push(client.service_name.clone());
                }
                Result::Ok(SendCmd::DelClient(client)) => {
                    // run hookers
                    for h in &mut self.hookers {
                        h.del_client(client.clone());
                    }

                    // emit event
                    let _ = self.event_tx.send(EventCmd::DelClient(client.clone()));

                    // remove service name
                    self.service_names.retain(|s| s != &client.service_name);
                }
                Result::Err(_) => { /* go on */ }
            }

            // handle output msg from core
            match self.core.recv().try_recv() {
                Ok(msg) => {
                    let msg = M::try_from(msg).unwrap();

                    // after exchange
                    for hooker in self.hookers.iter_mut() {
                        hooker.after_exchange(Cow::Borrowed(&msg));
                    }

                    // send to output
                    self.recv_tx.send(RecvCmd::OutputMsg(msg)).await?;
                }
                Result::Err(_) => { /* go on */ }
            }

            // closed
            match closed.try_recv() {
                Ok(_) => {
                    self.core.close()?;
                    for hooker in self.hookers.iter_mut() {
                        hooker.close();
                    }
                    break;
                }
                _ => { /* go on */ }
            }
        }

        Ok(())
    }

    fn get_commander(&mut self) -> (Sender<SendCmd<M>>, Receiver<RecvCmd<M>>) {
        (self.send_tx.take().unwrap(), self.recv_rx.take().unwrap())
    }

    fn get_event_recver(&mut self) -> broadcast::Receiver<EventCmd<M>> {
        self.event_rx.take().unwrap()
    }
}

// ==== Constructor Utility ====

pub fn with_memory<M>() -> Transmitter<MemoryExchange, M>
where
    M: MessageTrait,
{
    Transmitter::new(MemoryExchange::new())
}

pub fn with_redis<M>(uri: String) -> Transmitter<RedisExchange, M>
where
    M: MessageTrait,
{
    Transmitter::new(RedisExchange::new(uri))
}

// ==== OrTransmitter Impl ====

pub struct OrTransmitter<M>
where
    M: MessageTrait,
{
    core_memory: Option<Transmitter<MemoryExchange, M>>,
    core_redis: Option<Transmitter<RedisExchange, M>>,
}

pub struct Config {
    pub use_memory: bool,
    pub use_redis: bool,
    pub redis_uri: String,
}

impl<M> OrTransmitter<M>
where
    M: MessageTrait,
{
    pub fn new(config: Config) -> Self {
        Self {
            core_memory: if config.use_memory {
                Some(with_memory())
            } else {
                None
            },
            core_redis: if config.use_redis {
                Some(with_redis(config.redis_uri))
            } else {
                None
            },
        }
    }
}

impl<M> Transmit<M> for OrTransmitter<M>
where
    M: MessageTrait + 'static,
{
    fn init(&mut self) -> Result<()> {
        if let Some(core) = &mut self.core_memory {
            core.init()?;
        }
        if let Some(core) = &mut self.core_redis {
            core.init()?;
        }
        Ok(())
    }

    fn register_hooker(&mut self, hooker: Box<dyn Hook<M>>) {
        if let Some(core) = &mut self.core_memory {
            core.register_hooker(hooker);
        } else if let Some(core) = &mut self.core_redis {
            core.register_hooker(hooker);
        }
    }

    async fn run(&mut self, closed: broadcast::Receiver<()>) -> Result<()> {
        if let Some(core) = &mut self.core_memory {
            core.run(closed.resubscribe()).await?;
        }
        if let Some(core) = &mut self.core_redis {
            core.run(closed.resubscribe()).await?;
        }
        Ok(())
    }

    fn get_commander(&mut self) -> (Sender<SendCmd<M>>, Receiver<RecvCmd<M>>) {
        if let Some(core) = &mut self.core_memory {
            core.get_commander()
        } else if let Some(core) = &mut self.core_redis {
            core.get_commander()
        } else {
            unreachable!()
        }
    }

    fn get_event_recver(&mut self) -> broadcast::Receiver<EventCmd<M>> {
        if let Some(core) = &mut self.core_memory {
            core.get_event_recver()
        } else if let Some(core) = &mut self.core_redis {
            core.get_event_recver()
        } else {
            unreachable!()
        }
    }
}
