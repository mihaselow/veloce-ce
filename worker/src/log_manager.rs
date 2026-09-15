#![allow(clippy::all)]
use std::sync::OnceLock;
use tokio::sync::mpsc;
use veloce_common::Message;

pub static GLOBAL_LOG_TX: OnceLock<mpsc::Sender<Message>> = OnceLock::new();

pub enum LogManagerCmd {
    SetTx(mpsc::Sender<Message>),
}

pub struct LogManager {
    rx: mpsc::Receiver<Message>,
    current_tx: Option<mpsc::Sender<Message>>,
    ring_buffer: veloce_common::ring_buffer::RingBuffer<Message>,
}

impl LogManager {
    pub fn new(rx: mpsc::Receiver<Message>) -> Self {
        Self {
            rx,
            current_tx: None,
            ring_buffer: veloce_common::ring_buffer::RingBuffer::new(5000),
        }
    }

    pub async fn run(mut self, mut cmd_rx: mpsc::Receiver<LogManagerCmd>) {
        loop {
            tokio::select! {
                Some(msg) = self.rx.recv() => {
                    if let Some(tx) = &self.current_tx {
                        if tx.send(msg.clone()).await.is_err() {
                            self.current_tx = None;
                            self.ring_buffer.push(msg);
                        }
                    } else {
                        self.ring_buffer.push(msg);
                    }
                }
                Some(cmd) = cmd_rx.recv() => {
                    match cmd {
                        LogManagerCmd::SetTx(new_tx) => {
                            self.current_tx = Some(new_tx.clone());
                            let backfill = self.ring_buffer.pop_all();
                            for m in backfill {
                                let _ = new_tx.send(m).await;
                            }
                        }
                    }
                }
            }
        }
    }
}
