use crate::core::codec;
use crate::core::objectstore::*;
use crate::core::util::chunk_key;
use crate::core::wal::*;
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::Pending;
use std::collections::VecDeque;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

#[derive(Clone)]
struct ChunkInfo {
    seq: u64,
    path: String,
}

type AppendResponseTx = oneshot::Sender<Result<ChunkInfo, WALError>>;

struct PendingAppend {
    records: Vec<Record>,
    tx: AppendResponseTx,
}

enum AppendCommand {
    Append(PendingAppend),
    Flush,
    Shutdown,
}

enum ManifestCommand {
    Commit { next_seq: u64 },
    Update,
    Shutdown,
}

#[derive(Clone)]
struct ManifestInfo {
    etag: String,
    manifest: Manifest,
}

struct Buffer {
    pending: VecDeque<PendingAppend>,
    curr_size: usize,
}

impl Buffer {
    fn new() -> Self {
        Buffer {
            pending: VecDeque::new(),
            curr_size: 0,
        }
    }

    fn add(&mut self, pending_append: PendingAppend) -> Result<(), WALError> {
        let mut pending_size: usize = 0;
        pending_size += codec::records_size(&pending_append.records).map_err(|err| {
            WALError::Internal(format!(
                "failed to append pending records to buffer: {}",
                err.to_string()
            ))
        })?;

        self.pending.push_back(pending_append);
        self.curr_size += pending_size;

        Ok(())
    }

    fn curr_size(&self) -> usize {
        self.curr_size
    }

    fn can_flush(&self) -> bool {
        if self.curr_size >= codec::MAX_CHUNK_LEN {
            return true;
        }
        return false;
    }

    fn flush(&mut self) -> Vec<PendingAppend> {
        let mut output = Vec::new();
        output.extend(self.pending.drain(..));
        self.curr_size = 0;

        return output;
    }
}

/// WALWriter implements a WAL appender on top of object store using the
/// protocol described in https://nvartolomei.com/oswald/#appending.
struct AppenderImpl<'a, T>
where
    T: ObjectStoreWriter + ObjectStoreReader,
{
    store: &'a T,
    bucket: String,
    name: String,
    append_tx: mpsc::Sender<AppendCommand>,
    append_rx: Mutex<Option<mpsc::Receiver<AppendCommand>>>,
    buffer: Mutex<Buffer>,
    manifest: Mutex<Option<ManifestInfo>>,
}

#[async_trait]
impl<'a, T> Appender for AppenderImpl<'a, T>
where
    T: ObjectStoreWriter + ObjectStoreReader,
{
    async fn append(&self, cmd: AppendRequest) -> Result<AppendResponse, WALError> {
        let (tx, rx) = oneshot::channel::<Result<ChunkInfo, WALError>>();
        let append_tx = self.append_tx.clone();

        append_tx
            .send(AppendCommand::Append(PendingAppend {
                records: cmd.records,
                tx: tx,
            }))
            .await
            .map_err(|err| WALError::Internal(format!("failed to append: {}", err.to_string())))?;

        let result = rx.await.map_err(|err| {
            WALError::Internal(format!(
                "failed to receive append result: {}",
                err.to_string()
            ))
        })?;

        match result {
            Ok(chunk_info) => {
                return Ok(AppendResponse {
                    chunk_seq: chunk_info.seq,
                });
            }
            Err(err) => {
                return Err(err);
            }
        }
    }
}

impl<'a, T> AppenderImpl<'a, T>
where
    T: ObjectStoreWriter + ObjectStoreReader,
{
    async fn start_appender(&mut self) -> Result<(), WALError> {
        let mut rx = self
            .append_rx
            .lock()
            .await
            .take()
            .ok_or_else(|| WALError::Internal("appender already started".to_string()))?;

        while let Some(cmd) = rx.recv().await {
            match cmd {
                AppendCommand::Append(pending_append) => {
                    let curr_records_size =
                        codec::records_size(&pending_append.records).map_err(|err| {
                            WALError::Internal(format!(
                                "failed to calculate records size: {}",
                                err.to_string()
                            ))
                        })?;
                    let mut buffer = self.buffer.lock().await;
                    if buffer.curr_size() + curr_records_size >= codec::MAX_CHUNK_LEN {
                        let pending = buffer.flush();
                        self.flush_pending_appends(pending).await?;
                    }

                    buffer.add(pending_append)?;
                    if buffer.can_flush() {
                        let pending = buffer.flush();
                        self.flush_pending_appends(pending).await?;
                    }
                }
                AppendCommand::Flush => {
                    let mut buffer = self.buffer.lock().await;
                    let pending = buffer.flush();
                    if pending.is_empty() {
                        continue;
                    }
                    self.flush_pending_appends(pending).await?;
                }
                AppendCommand::Shutdown => break,
            }
        }
        Ok(())
    }

    async fn flush_pending_appends(
        &self,
        pending_appends: Vec<PendingAppend>,
    ) -> Result<(), WALError> {
        let mut records = Vec::new();
        let mut callbacks = Vec::new();
        for pending_append in pending_appends {
            records.extend(pending_append.records);
            callbacks.push(pending_append.tx);
        }
        let encoded = codec::encode_records(&records).map_err(|err| {
            WALError::Internal(format!("unable to encode records: {}", err.to_string()))
        })?;

        let flush_result = self.flush_chunk(Bytes::from(encoded)).await;

        for callback in callbacks {
            let _ = callback.send(flush_result.clone());
        }

        Ok(())
    }

    async fn flush_chunk(&self, encoded: Bytes) -> Result<ChunkInfo, WALError> {
        let next_seq = {
            self.manifest
                .lock()
                .await
                .as_ref()
                .unwrap()
                .manifest
                .next_seq
        };

        let result = self
            .store
            .put_object(PubObjectRequest {
                bucket: self.bucket.to_owned(),
                key: chunk_key(&self.name, next_seq),
                body: WriteObjectBody::Bytes(encoded),
                condition: Some(PutObjectCondition::IfNoneMatch),
            })
            .await;
        match result {
            Err(err) => match err {
                ObjectStoreError::ObjectAlreadyExists(err) => {}
                _ => {
                    return Err(WALError::Internal("implement this".to_owned()));
                }
            },
            Ok(response) => {}
        }

        Err(WALError::Internal("implement this".to_owned()))
    }
}
