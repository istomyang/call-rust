use super::{Write, WriteRecvCmd, WriteSendCmd};
use anyhow::{Ok, Result};
use std::fs::OpenOptions;
use tokio::sync::mpsc::{self, Receiver, Sender};

pub(in crate::trace) struct WriteCsv {
    file_path: String,

    send_tx: Option<Sender<WriteSendCmd>>,
    send_rx: Receiver<WriteSendCmd>,

    recv_tx: Sender<WriteRecvCmd>,
    recv_rx: Option<Receiver<WriteRecvCmd>>,
}

impl WriteCsv {
    pub(in crate::trace) fn new(file_path: String) -> Self {
        let (send_tx, send_rx) = mpsc::channel(32);
        let (recv_tx, recv_rx) = mpsc::channel(32);
        Self {
            file_path,
            send_tx: Some(send_tx),
            send_rx,
            recv_tx,
            recv_rx: Some(recv_rx),
        }
    }
}

impl Write for WriteCsv {
    async fn run(&mut self) -> Result<()> {
        loop {
            match self.send_rx.recv().await {
                Some(WriteSendCmd::RecordBatch(blocks)) => {
                    match OpenOptions::new()
                        .append(true)
                        .open(self.file_path.as_str())
                    {
                        Result::Ok(file) => {
                            let mut wtr = csv::Writer::from_writer(file);
                            for block in blocks.as_ref() {
                                if let Err(e) = wtr.serialize(block) {
                                    self.recv_tx.send(WriteRecvCmd::Err(e.into())).await?;
                                }
                            }
                            if let Err(e) = wtr.flush() {
                                self.recv_tx.send(WriteRecvCmd::Err(e.into())).await?;
                            }
                        }
                        _ => {}
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
