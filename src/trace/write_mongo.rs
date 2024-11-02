use super::{TraceBlock, Write, WriteRecvCmd, WriteSendCmd};
use anyhow::{Ok, Result};
use tokio::sync::mpsc::{self, Receiver, Sender};

pub(in crate::trace) struct WriteMongo {
    mongo_url: String,

    send_tx: Option<Sender<WriteSendCmd>>,
    send_rx: Receiver<WriteSendCmd>,

    recv_tx: Sender<WriteRecvCmd>,
    recv_rx: Option<Receiver<WriteRecvCmd>>,
}

impl WriteMongo {
    pub(in crate::trace) fn new(mongo_url: String) -> Self {
        let (send_tx, send_rx) = mpsc::channel(32);
        let (recv_tx, recv_rx) = mpsc::channel(32);
        Self {
            mongo_url,
            send_tx: Some(send_tx),
            send_rx,
            recv_tx,
            recv_rx: Some(recv_rx),
        }
    }
}

impl Write for WriteMongo {
    async fn run(&mut self) -> Result<()> {
        let client = mongodb::Client::with_uri_str(self.mongo_url.as_str()).await?;
        let db = client.database("call-rust");
        let collection = db.collection::<TraceBlock>("trace_log");

        loop {
            match self.send_rx.recv().await {
                Some(WriteSendCmd::RecordBatch(blocks)) => {
                    match collection.insert_many(blocks.as_ref()).await {
                        Result::Ok(_) => {}
                        Err(e) => {
                            self.recv_tx.send(WriteRecvCmd::Err(e.into())).await?;
                        }
                    }
                }
                Some(WriteSendCmd::Close) => {
                    break;
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn send(&mut self) -> Sender<WriteSendCmd> {
        self.send_tx.take().unwrap()
    }

    fn recv(&mut self) -> Receiver<WriteRecvCmd> {
        self.recv_rx.take().unwrap()
    }

    fn init(&mut self) -> Result<()> {
        Ok(())
    }
}
