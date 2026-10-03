use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::{
    sync::{Mutex, mpsc, oneshot, watch},
    task::JoinSet,
    time::Instant,
};

use super::common::store_error;
use super::manifest::{ManifestCommand, ManifestCoordinator};
use crate::core::{codec, objectstore::*, util::chunk_key, wal::*};

/// Buffering settings for an appender.
#[derive(Clone, Copy, Debug)]
pub struct AppenderConfig {
    /// Maximum buffering time from the first queued request in a chunk.
    /// Later requests do not reset this deadline. Zero flushes immediately.
    /// Queueing and object-store I/O may delay completion beyond this timeout.
    pub flush_timeout: Duration,
}

impl Default for AppenderConfig {
    fn default() -> Self {
        Self {
            flush_timeout: Duration::from_millis(100),
        }
    }
}

struct PendingAppend {
    records: Vec<Record>,
    size: usize,
    deadline: Instant,
    response: oneshot::Sender<Result<u64, WALError>>,
}

/// Buffers requests into immutable chunks up to the codec byte or record limit.
/// A request is never split; if it will not fit, the preceding chunk is flushed.
/// Partial chunks flush after the configured timeout (100 ms by default).
/// Share this instance, e.g. through `Arc`, to batch concurrent append calls.
///
/// The bucket must already exist; the manifest is created on the first flush.
/// Success means the chunk is durable and queued for manifest publication;
/// tailers may observe it later. Dropping an
/// append future after enqueueing does not cancel its write. Call `run` explicitly
/// and drive it concurrently with append calls. Call `close` and await `run` to
/// drain accepted writes and publish their manifest entries before shutdown.
/// `run` supervises both tasks and reports failures, including publication errors
/// after an append has succeeded. Dropping `run` cancels both tasks;
/// uncommitted requests then fail rather than being silently restarted.
/// Cancellation or an I/O error may leave a durable chunk: retrying an append is
/// not idempotent. A subsequent append recovers unpublished chunks.
pub struct AppenderImpl<T> {
    store: Arc<T>,
    bucket: String,
    name: String,
    config: AppenderConfig,
    sender: Mutex<Option<mpsc::Sender<PendingAppend>>>,
    receiver: Mutex<Option<mpsc::Receiver<PendingAppend>>>,
}

impl<T> AppenderImpl<T> {
    pub fn new(store: Arc<T>, bucket: impl Into<String>, name: impl Into<String>) -> Self {
        Self::with_config(store, bucket, name, AppenderConfig::default())
    }

    pub fn with_config(
        store: Arc<T>,
        bucket: impl Into<String>,
        name: impl Into<String>,
        config: AppenderConfig,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(128);
        Self {
            store,
            bucket: bucket.into(),
            name: name.into(),
            config,
            sender: Mutex::new(Some(sender)),
            receiver: Mutex::new(Some(receiver)),
        }
    }

    /// Stop accepting new calls. Calls already holding a queue sender may finish
    /// enqueueing; `run` drains those requests and returns. Await `run` separately
    /// to observe completion. This is idempotent and also works before `run`.
    pub async fn close(&self) {
        self.sender.lock().await.take();
    }
}

#[async_trait]
impl<T: ObjectStoreWriter + ObjectStoreReader + 'static> Appender for AppenderImpl<T> {
    async fn run(&self) -> Result<(), WALError> {
        let receiver = self
            .receiver
            .lock()
            .await
            .take()
            .ok_or_else(|| WALError::BadRequest("appender can only be run once".into()))?;
        let (commands, command_rx) = mpsc::channel(128);
        let (state_tx, state) = watch::channel(None);
        let coordinator = ManifestCoordinator::new(
            self.store.clone(),
            self.bucket.clone(),
            self.name.clone(),
            state_tx,
        );
        let worker = ChunkWriter {
            store: self.store.clone(),
            bucket: self.bucket.clone(),
            name: self.name.clone(),
            commands,
            state,
            next_seq: None,
        };
        // Dropping JoinSet aborts both children if the caller cancels `run`.
        let mut tasks = JoinSet::new();
        tasks.spawn(coordinator.run(command_rx));
        tasks.spawn(worker.run(receiver));
        while let Some(result) = tasks.join_next().await {
            let result = result
                .unwrap_or_else(|e| Err(WALError::Internal(format!("appender task failed: {e}"))));
            if let Err(error) = result {
                tasks.abort_all();
                self.close().await;
                tasks.shutdown().await;
                return Err(error);
            }
        }
        Ok(())
    }

    async fn append(&self, cmd: AppendRequest) -> Result<AppendResponse, WALError> {
        let size = codec::chunk_payload_size(&cmd.records)
            .map_err(|e| WALError::BadRequest(e.to_string()))?;
        let sender = self
            .sender
            .lock()
            .await
            .clone()
            .ok_or_else(|| WALError::Internal("appender is closed".into()))?;
        let deadline = Instant::now()
            .checked_add(self.config.flush_timeout)
            .ok_or_else(|| WALError::BadRequest("flush timeout is too large".into()))?;
        let (response, result) = oneshot::channel();
        sender
            .send(PendingAppend {
                records: cmd.records,
                size,
                deadline,
                response,
            })
            .await
            .map_err(|_| WALError::Internal("appender worker stopped".into()))?;
        drop(sender);
        let chunk_seq = result.await.map_err(|_| {
            WALError::Internal("appender worker stopped before acknowledgement".into())
        })??;
        Ok(AppendResponse { chunk_seq })
    }
}

struct ChunkWriter<T> {
    store: Arc<T>,
    bucket: String,
    name: String,
    commands: mpsc::Sender<ManifestCommand>,
    state: watch::Receiver<Option<Manifest>>,
    next_seq: Option<u64>,
}

impl<T: ObjectStoreWriter + ObjectStoreReader> ChunkWriter<T> {
    async fn run(mut self, mut rx: mpsc::Receiver<PendingAppend>) -> Result<(), WALError> {
        let mut records = Vec::new();
        let mut responses = Vec::new();
        let mut size = 0;
        let mut deadline = None;
        let mut deferred = None;
        loop {
            let pending = if let Some(pending) = deferred.take() {
                Some(pending)
            } else if let Some(at) = deadline {
                tokio::select! {
                    biased;
                    _ = tokio::time::sleep_until(at) => {
                        // Uploads can outlast queued requests' deadlines. Take a
                        // bounded snapshot of already queued work before flushing,
                        // without waiting for more arrivals or resetting the timer.
                        for _ in 0..rx.len().min(128) {
                            let Ok(pending) = rx.try_recv() else { break; };
                            if size + pending.size > codec::MAX_KEY_VALUE_LEN
                                || records.len() + pending.records.len() > codec::MAX_NUM_RECORDS
                            {
                                deferred = Some(pending);
                                break;
                            }
                            size += pending.size;
                            records.extend(pending.records);
                            responses.push(pending.response);
                            if size == codec::MAX_KEY_VALUE_LEN || records.len() == codec::MAX_NUM_RECORDS {
                                break;
                            }
                        }
                        self.flush(&mut records, &mut responses).await?;
                        size = 0;
                        deadline = None;
                        continue;
                    }
                    pending = rx.recv() => pending,
                }
            } else {
                rx.recv().await
            };
            let Some(pending) = pending else {
                self.flush(&mut records, &mut responses).await?;
                return Ok(());
            };
            if size + pending.size > codec::MAX_KEY_VALUE_LEN
                || records.len() + pending.records.len() > codec::MAX_NUM_RECORDS
            {
                self.flush(&mut records, &mut responses).await?;
                size = 0;
                deadline = None;
            }
            if deadline.is_none() {
                deadline = Some(pending.deadline);
            }
            size += pending.size;
            records.extend(pending.records);
            responses.push(pending.response);
            if size == codec::MAX_KEY_VALUE_LEN || records.len() == codec::MAX_NUM_RECORDS {
                self.flush(&mut records, &mut responses).await?;
                size = 0;
                deadline = None;
            }
        }
    }

    async fn flush(
        &mut self,
        records: &mut Vec<Record>,
        responses: &mut Vec<oneshot::Sender<Result<u64, WALError>>>,
    ) -> Result<(), WALError> {
        if records.is_empty() {
            return Ok(());
        }
        let result = match codec::encode_records(records) {
            Ok(encoded) => self.persist(Bytes::from(encoded)).await,
            Err(e) => Err(WALError::Internal(e.to_string())),
        };
        records.clear();
        for response in responses.drain(..) {
            let _ = response.send(result.clone());
        }
        result.map(|_| ())
    }

    async fn persist(&mut self, body: Bytes) -> Result<u64, WALError> {
        if self.next_seq.is_none() {
            self.commands
                .send(ManifestCommand::Refresh)
                .await
                .map_err(|_| WALError::Internal("manifest coordinator stopped".into()))?;
            let manifest = *self
                .state
                .wait_for(|state| state.is_some())
                .await
                .map_err(|_| WALError::Internal("manifest coordinator stopped".into()))?;
            self.next_seq = Some(manifest.unwrap().next_seq);
        }
        loop {
            if self.commands.is_closed() {
                return Err(WALError::Internal("manifest coordinator stopped".into()));
            }
            let published = self.state.borrow().map(|m| m.next_seq).unwrap_or(0);
            let seq = self.next_seq.unwrap().max(published);
            let next = seq
                .checked_add(1)
                .ok_or_else(|| WALError::BadRequest("sequence number exhausted".into()))?;
            let owned = match self
                .store
                .put_object(PubObjectRequest {
                    bucket: self.bucket.clone(),
                    key: chunk_key(&self.name, seq),
                    body: WriteObjectBody::Bytes(body.clone()),
                    condition: Some(PutObjectCondition::IfNoneMatch),
                })
                .await
            {
                Ok(_) => true,
                Err(
                    ObjectStoreError::ObjectAlreadyExists(_)
                    | ObjectStoreError::PreConditionFailed(_),
                ) => {
                    // Recover an occupied slot only after confirming the object
                    // exists. Never publish past an unverified hole.
                    match self
                        .store
                        .get_object(GetObjectRequest {
                            bucket: self.bucket.clone(),
                            key: chunk_key(&self.name, seq),
                            if_none_match: None,
                        })
                        .await
                        .map_err(store_error)?
                    {
                        GetObjectResult::Found(_) => false,
                        GetObjectResult::NotModified => {
                            return Err(WALError::Internal("unexpected unchanged chunk".into()));
                        }
                    }
                }
                Err(ObjectStoreError::Conflict(_)) => {
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(e) => return Err(store_error(e)),
            };
            // Sequence allocation progresses independently of manifest I/O.
            self.next_seq = Some(next);
            self.commands
                .send(ManifestCommand::PublishThrough { chunk_seq: seq })
                .await
                .map_err(|_| WALError::Internal("manifest coordinator stopped".into()))?;
            if owned {
                return Ok(seq);
            }
        }
    }
}
