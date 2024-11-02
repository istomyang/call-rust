use super::Exchange;
use anyhow::{Error, Ok, Result};
use redis::Commands;
use tokio::sync::oneshot;

pub struct RedisExchange {
    uri: String,

    tx: Option<oneshot::Sender<String>>,
    rx: Option<oneshot::Receiver<String>>,

    connection: Option<redis::Connection>,
}

impl RedisExchange {
    pub fn new(uri: String) -> Self {
        let (tx, rx) = oneshot::channel();
        Self {
            uri,
            tx: Some(tx),
            rx: Some(rx),
            connection: None,
        }
    }

    pub fn key() -> &'static str {
        "call-rust:transmit-redis"
    }
}

impl Exchange for RedisExchange {
    fn init(&mut self) -> Result<()> {
        let client = redis::Client::open(self.uri.as_str())?;
        let con = client.get_connection()?;
        self.connection = Some(con);
        Ok(())
    }

    fn nonblocking_run(&mut self) -> Result<()> {
        use redis::RedisResult;

        match self.connection.as_mut() {
            Some(c) => match c.lpop(Self::key(), None) {
                RedisResult::Ok(msg) => {
                    let _ = self.tx.take().unwrap().send(msg);
                }
                RedisResult::Err(e) => {
                    return Err(e.into());
                }
            },
            None => {
                return Ok(());
            }
        }

        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn send(&mut self, data: String) -> Result<()> {
        match self.connection.as_mut() {
            Some(c) => c
                .lpush::<&str, String, ()>(Self::key(), data)
                .map_err(Error::from),
            None => Ok(()),
        }
    }

    fn recv(&mut self) -> oneshot::Receiver<String> {
        self.rx.take().unwrap()
    }
}
