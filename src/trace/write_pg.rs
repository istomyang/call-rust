use super::{Write, WriteRecvCmd, WriteSendCmd};
use anyhow::{Ok, Result};
use futures_util::pin_mut;
use tokio::{
    fs::File,
    io::AsyncReadExt,
    sync::mpsc::{self, Receiver, Sender},
};
use tokio_postgres::{binary_copy::BinaryCopyInWriter, connect, types::Type, NoTls};

pub(in crate::trace) struct WritePg {
    pg_config: String,

    send_tx: Option<Sender<WriteSendCmd>>,
    send_rx: Receiver<WriteSendCmd>,

    recv_tx: Sender<WriteRecvCmd>,
    recv_rx: Option<Receiver<WriteRecvCmd>>,
}

impl WritePg {
    pub(in crate::trace) fn new(pg_config: String) -> Self {
        let (send_tx, send_rx) = mpsc::channel(32);
        let (recv_tx, recv_rx) = mpsc::channel(32);
        Self {
            pg_config,
            send_tx: Some(send_tx),
            send_rx,
            recv_tx,
            recv_rx: Some(recv_rx),
        }
    }
}

impl WritePg {
    async fn read_files(path: &str) -> Result<String> {
        let mut file = File::open(path).await?;
        let mut sql = String::new();
        file.read_to_string(&mut sql).await?;
        Ok(sql)
    }
}

impl Write for WritePg {
    async fn run(&mut self) -> Result<()> {
        let (client, connection) = connect(self.pg_config.as_str(), NoTls).await?;

        let copy = Self::read_files("sql/trace_log.sql").await?;
        let create = Self::read_files("sql/trace_log.sql").await?;

        client.batch_execute(create.as_str()).await?;

        {
            let tx = self.send_tx.as_mut().unwrap().clone();
            tokio::spawn(async move {
                if let Err(_) = connection.await {
                    tx.send(WriteSendCmd::Close).await.unwrap();
                }
            });
        }

        loop {
            match self.send_rx.recv().await {
                Some(WriteSendCmd::RecordBatch(blocks)) => {
                    let sink = client.copy_in(copy.as_str()).await?;
                    let writer = BinaryCopyInWriter::new(
                        sink,
                        &[
                            Type::VARCHAR,
                            Type::VARCHAR,
                            Type::TIMESTAMP,
                            Type::VARCHAR,
                            Type::VARCHAR,
                            Type::VARCHAR,
                            Type::VARCHAR,
                            Type::BOOL,
                            Type::BOOL,
                            Type::VARCHAR,
                        ],
                    );
                    pin_mut!(writer);
                    for a in blocks.as_ref() {
                        writer
                            .as_mut()
                            .write(&[
                                &a.msg_id,
                                &a.request_id,
                                &a.created_at,
                                &a.from_service_name,
                                &a.from_service_no,
                                &a.to_service_name,
                                &a.to_service_no,
                                &a.is_request,
                                &a.write_op,
                                &a.success,
                                &a.description,
                            ])
                            .await?;
                    }
                    writer.finish().await?;
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
        self.send_tx.as_mut().unwrap().clone()
    }

    fn recv(&mut self) -> Receiver<WriteRecvCmd> {
        self.recv_rx.take().unwrap()
    }

    fn init(&mut self) -> Result<()> {
        Ok(())
    }
}
