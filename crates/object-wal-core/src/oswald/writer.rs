use super::*;
use crate::objectstore::ports::{GetObjectRequest, ObjectStoreReader, ObjectStoreWriter};
use crate::types::*;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{MissedTickBehavior, interval};

pub struct WALWriter<T>
where
    T: ObjectStoreWriter + ObjectStoreReader,
{
    store: T,
    current_seq_num: u64,
    bucket_name: String,
    key_prefix: String,
    manifest_etag: Option<String>,
    buffer: Arc<Mutex<Vec<Record>>>,
    tx: mpsc::Sender<WriteCommand>,
    rx: Mutex<mpsc::Receiver<WriteCommand>>,
}

enum WriteCommand {
    FlushChunk,
    Shutdown,
}

impl<T> WALWriter<T>
where
    T: ObjectStoreWriter + ObjectStoreReader + 'static,
{
    pub fn new(store: T, bucket_name: &str, key_prefix: &str) -> Self {
        let (tx, rx) = mpsc::channel::<WriteCommand>(32);

        WALWriter {
            store: store,
            current_seq_num: 1,
            bucket_name: bucket_name.to_owned(),
            manifest_etag: None,
            key_prefix: key_prefix.to_owned(),
            buffer: Arc::new(Mutex::new(Vec::new())),
            tx: tx,
            rx: Mutex::new(rx),
        }
    }

    pub async fn start(self: &Arc<Self>) -> Result<(), WALError> {
        // let mut handles = vec![];
        // let writer: Arc<WALWriter<T>> = Arc::clone(self);
        tokio::try_join!(self.write_chunk())?;
        // handles.push(tokio::spawn(async move { writer.write_chunk().await }));
        Ok(())
    }

    async fn write_chunk(&self) -> Result<(), WALError> {
        let mut ticker = interval(Duration::from_millis(250));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let mut rx = self
            .rx
            .lock()
            .map_err(|err| WALError::Internal(err.to_string()))?;

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    self.flush_chunk_if_needed(true).await?;
                },
                 cmd = rx.recv() =>  {
                    match cmd {
                        Some(cmd) => {
                            match cmd {
                                WriteCommand::FlushChunk => {
                                    self.flush_chunk_if_needed(false).await?;
                                },
                                WriteCommand::Shutdown => {
                                    break;
                                },
                            }
                        },
                        None => {
                            break;
                        },
                    }
                 },
            }
        }

        Err(WALError::Internal(format!("not implemented")))
    }

    async fn flush_chunk_if_needed(&self, timer_expired: bool) -> Result<(), WALError> {
        let records = {
            let mut buffer = self
                .buffer
                .lock()
                .map_err(|err| WALError::Internal(err.to_string()))?;
            if buffer.is_empty() {
                return Ok(());
            }
            std::mem::take(&mut *buffer);
        };
        Err(WALError::Internal(format!("not implemented")))
    }

    fn get_next_chunk_key(&self) -> String {
        format!("<date>/chunk<seqnum>")
    }

    pub async fn append(&self, cmd: AppendRequest) -> Result<AppendResponse, WALError> {
        let buffer = Arc::clone(&self.buffer);
        buffer
            .lock()
            .map_err(|err| WALError::Internal(err.to_string()))?
            .extend(cmd.records);

        let tx = self.tx.clone();
        tx.send(WriteCommand::FlushChunk)
            .await
            .map_err(|err| WALError::Internal(err.to_string()))?;

        Ok(AppendResponse {})
    }
}
