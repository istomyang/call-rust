use super::Exchange;
use anyhow::{Ok, Result};
use tokio::sync::oneshot;

pub struct MemoryExchange {
    tx: Option<oneshot::Sender<String>>,
    rx: Option<oneshot::Receiver<String>>,
}

impl MemoryExchange {
    pub fn new() -> Self {
        let (tx, rx) = oneshot::channel();
        Self {
            tx: Some(tx),
            rx: Some(rx),
        }
    }
}

impl Exchange for MemoryExchange {
    fn init(&mut self) -> Result<()> {
        Ok(())
    }

    fn nonblocking_run(&mut self) -> Result<()> {
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn send(&mut self, data: String) -> Result<()> {
        let _ = self.tx.take().unwrap().send(data);
        Ok(())
    }

    fn recv(&mut self) -> oneshot::Receiver<String> {
        self.rx.take().unwrap()
    }
}
