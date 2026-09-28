use super::*;
use crate::objectstore::ports::*;
use crate::types::{self, *};
use crate::{codec, objectstore};
use async_trait::async_trait;
use std::collections::VecDeque;
use std::time::Duration;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::time::{MissedTickBehavior, interval};
use tokio_retry::strategy::{ExponentialBackoff, jitter};

type AppendResponseTx = oneshot::Sender<Result<ChunkAppendInfo, WALError>>;

struct PendingAppend {
    records: Vec<Record>,
    response: AppendResponseTx,
}

struct WALState {
    manifest_etag: Option<String>,
    manifest: Manifest,
    curr_seq: u64,
    pending_appends: VecDeque<PendingAppend>,
    bucket_name: String,
    retry_batch: Option<UploadBatch>,
}

impl WALState {
    async fn new(store: &impl ObjectStoreReader, bucket_name: String) -> Result<Self, WALError> {
        // let response: ManifestResponse =
        //     get_manifest(store, bucket_name.as_str(), MANIFEST_KEY, None).await?;
        // let curr_seq = response.manifest.curr_seq;
        Ok(Self {
            // store: store,
            // manifest_etag: response.etag,
            // manifest: response.manifest,
            manifest: Manifest {
                watermark_seq: 0,
                curr_seq: 0,
                // current_snapshot: None,
            },
            manifest_etag: None,
            curr_seq: 0,
            pending_appends: VecDeque::new(),
            bucket_name: bucket_name,
            retry_batch: None,
        })
    }
}

struct ChunkSequence {
    seq: u64,
    path: String,
}

struct UploadBatch {
    records: Vec<Record>,
    responses: Vec<AppendResponseTx>,
    seq: Option<ChunkSequence>,
}

impl UploadBatch {
    fn new(records: Vec<Record>, responses: Vec<AppendResponseTx>) -> Self {
        UploadBatch {
            records,
            responses,
            seq: None,
        }
    }
    fn set_seq(&mut self, seq: u64, key_prefix: &str) {
        self.seq = Some(ChunkSequence {
            seq: seq,
            path: format!("{}/chunk-{:020}", key_prefix.to_string(), seq),
        })
    }
}

enum ManifestCommand {
    Refresh,
    Commit {
        chunk_seq: u64,
        chunk_path: String,
        sync: bool,
    },
    Shutdown,
}

struct CurrentManifest {}

struct ChunkAppendInfo {
    pub chunk_path: Option<String>,
}

enum AppendCommand {
    Append {
        records: Vec<Record>,
        response: oneshot::Sender<Result<ChunkAppendInfo, WALError>>,
    },
    Flush,
    Shutdown,
}

enum FlushResponse {}

pub struct OswaldWAL<T>
where
    T: ObjectStoreWriter + ObjectStoreReader,
{
    store: T,
    bucket_name: String,
    key_prefix: String,
    append_tx: mpsc::Sender<AppendCommand>,
    append_rx: Mutex<Option<mpsc::Receiver<AppendCommand>>>,
    manifest_tx: mpsc::Sender<ManifestCommand>,
    manifest_rx: Mutex<Option<mpsc::Receiver<ManifestCommand>>>,
    manifest_etag: Mutex<Option<String>>,
}

#[async_trait]
impl<T> types::WALWriter for OswaldWAL<T>
where
    T: ObjectStoreReader + ObjectStoreWriter,
{
    async fn append(&self, cmd: AppendRequest) -> Result<AppendResponse, WALError> {
        let chunk_size = codec::records_size(&cmd.records)
            .map_err(|err| WALError::BadRequest(err.to_string()))?;
        if chunk_size > codec::MAX_CHUNK_LEN {
            return Err(WALError::BadRequest(format!("chunk size exceeds limt")));
        }

        let (response_tx, response_rx) = oneshot::channel();
        self.append_tx
            .send(AppendCommand::Append {
                records: cmd.records,
                response: response_tx,
            })
            .await
            .map_err(|err| WALError::Internal(format!("failed to append : {}", err.to_string())))?;

        let chunk_info = response_rx.await.map_err(|err| {
            WALError::Internal(format!("failed to append: {}", err.to_string()))
        })??;

        Ok(AppendResponse {
            chunk_path: chunk_info.chunk_path,
        })
    }
}

impl<T> OswaldWAL<T>
where
    T: ObjectStoreWriter + ObjectStoreReader,
{
    pub fn new(store: T, bucket_name: &str, key_prefix: &str) -> Self {
        let (append_tx, append_rx) = mpsc::channel::<AppendCommand>(32);
        let (manifest_tx, manifest_rx) = mpsc::channel::<ManifestCommand>(4);

        OswaldWAL {
            store: store,
            bucket_name: bucket_name.to_owned(),
            key_prefix: key_prefix.to_owned(),
            append_tx: append_tx,
            append_rx: Mutex::new(Some(append_rx)),
            manifest_tx: manifest_tx,
            manifest_rx: Mutex::new(Some(manifest_rx)),
            manifest_etag: Mutex::new(None),
        }
    }

    pub async fn start_manifest_worker(&self) -> Result<(), WALError> {
        let mut rx = self
            .manifest_rx
            .lock()
            .await
            .take()
            .ok_or(WALError::Internal(format!(
                "manifest worker already started"
            )))?;

        let mut ticker = interval(Duration::from_millis(250));

        loop {
            tokio::select! {
                cmd = rx.recv() => {
                    match cmd {
                        Some(ManifestCommand::Refresh) => {

                        },
                        Some(ManifestCommand::Commit{
                            chunk_seq,
                            chunk_path,
                            sync,
                        }) => {
                            if sync {

                            }
                        },
                        Some(ManifestCommand::Shutdown) | None => {
                            break;
                        },

                    }
                },
                _ = ticker.tick() => {},

            }
        }

        // while let Some(cmd) = rx.recv().await {
        //     match cmd {
        //         ManifestCommand::Commit {
        //             chunk_seq,
        //             chunk_path,
        //             sync: bool,
        //         } => {}
        //         ManifestCommand::Shutdown => break,
        //     }
        // }

        Ok(())
    }

    pub async fn start_append_worker(&self) -> Result<(), WALError> {
        Err(WALError::Internal(format!("not implemented")))
    }

    pub async fn start(&self) -> Result<(), WALError> {
        let mut rx = self
            .append_rx
            .lock()
            .await
            .take()
            .ok_or_else(|| WALError::Internal(format!("already started")))?;
        let mut state = WALState::new(&self.store, self.bucket_name.to_string()).await?;
        while let Some(cmd) = rx.recv().await {
            match cmd {
                AppendCommand::Append { records, response } => {
                    state
                        .pending_appends
                        .push_back(PendingAppend { records, response });
                }
                AppendCommand::Flush => {}
                AppendCommand::Shutdown => break,
            }
        }

        // let mut rx = &self.rx;

        // while let Some(cmd) = self.rx.recv().await {}

        // // let mut handles = vec![];
        // // let writer: Arc<WALWriter<T>> = Arc::clone(self);
        // tokio::try_join!(self.write_chunk())?;
        // handles.push(tokio::spawn(async move { writer.write_chunk().await }));
        Ok(())
    }

    async fn refresh_manifest(&self, state: &mut WALState) -> Result<(), WALError> {
        // let response = get_manifest(
        //     &self.store,
        //     self.bucket_name.as_str(),
        //     self.get_manifest_key().as_str(),
        //     state.manifest_etag.as_deref(),
        // )
        // .await?;

        // // self.manifest_etag.lock().await.replace();

        // state.curr_seq = response.manifest.curr_seq;
        // state.manifest_etag = response.etag;
        // state.manifest = response.manifest;

        Ok(())
    }

    async fn commit_manifest(&self, seq: u64, path: String) -> Result<String, WALError> {
        // put_manifest(&self.store, &self.bucket_name, etag, manifest_key, manifest)

        Err(WALError::Internal(format!("not implemented")))
    }

    async fn put_manifest(&self, state: &mut WALState) -> Result<String, WALError> {
        put_manifest(
            &self.store,
            &self.bucket_name,
            state.manifest_etag.as_deref(),
            self.get_manifest_key().as_str(),
            &state.manifest,
        )
        .await
    }

    fn take_upload_batch(&self, state: &mut WALState) -> Result<Option<UploadBatch>, WALError> {
        if let Some(batch) = state.retry_batch.take() {
            return Ok(Some(batch));
        }

        let appends = take_flush_batch(&mut state.pending_appends, codec::MAX_CHUNK_LEN)?;

        if appends.is_empty() {
            return Ok(None);
        }

        Ok(Some(prepare_upload_batch(appends)))
    }

    async fn upload_batch(&self, batch: &UploadBatch) -> Result<(), WALError> {
        let encoded = codec::encode_records(&batch.records)
            .map_err(|err| WALError::Internal(err.to_string()))?;
        let body = objectstore::util::bytes_to_stream(encoded)
            .map_err(|err| WALError::Internal(err.to_string()))?;

        let Some(seq) = batch.seq.as_ref() else {
            return Err(WALError::Internal(format!("seq missing from upload batch")));
        };

        self.store
            .put_object(PubObjectRequest {
                bucket: self.bucket_name.to_string(),
                key: seq.path.to_owned(),
                body: body,
                condition: Some(PutObjectCondition::IfNoneMatch),
            })
            .await
            .map_err(|err| {
                WALError::Internal(format!("failed to upload chunk: {}", err.to_string()))
            })?;

        Ok(())
    }

    async fn flush_once(&self, state: &mut WALState) -> Result<(), WALError> {
        // take a upload batch
        let Some(mut batch) = self.take_upload_batch(state)? else {
            return Ok(());
        };

        // set sequence from state
        let curr_seq = state.curr_seq + 1;
        batch.set_seq(curr_seq, &self.key_prefix);
        let curr_chunk_path = batch.seq.as_ref().unwrap().path.to_owned();

        // try upload batch
        let result = self.upload_batch(&batch).await;

        // return batch to state if case of error
        if let Err(err) = result {
            state.retry_batch = Some(batch);
            return Err(err);
        }

        // update manifest
        state.curr_seq = curr_seq;
        let curr_manifest = &mut state.manifest;
        curr_manifest.curr_seq = curr_seq;
        // curr_manifest.current_snapshot = Some(Snapshot {
        //     seq_num: curr_seq,
        //     chunk_path: curr_chunk_path,
        // });
        // put manifest
        let etag = self.put_manifest(state).await?;
        // let Some(etag) = manifest_update else {
        //     return Err(err);
        // };
        // update manifest etag in state
        state.manifest_etag = Some(etag);

        Ok(())
    }

    async fn flush_chunk(&self, state: &mut WALState) -> Result<(), WALError> {
        let mut delays = ExponentialBackoff::from_millis(100).map(jitter).take(3);

        loop {
            match self.flush_once(state).await {
                Ok(()) => break Ok(()),
                Err(err) => match delays.next() {
                    Some(delay) => tokio::time::sleep(delay).await,
                    None => break Err(err),
                },
            }
        }
    }

    fn get_next_chunk_key(&self, next_seq: u64) -> String {
        format!("{}/chunk-{:020}", self.key_prefix.to_string(), next_seq,)
    }

    fn get_manifest_key(&self) -> String {
        format!("{}/{}", self.key_prefix.to_string(), MANIFEST_KEY)
    }
}

fn take_flush_batch(
    pending: &mut VecDeque<PendingAppend>,
    max_chunk_size: usize,
) -> Result<Vec<PendingAppend>, WALError> {
    let mut selected = Vec::new();
    let mut selected_size: usize = 0;
    while let Some(first) = pending.front() {
        let curr_size = codec::records_size(&first.records)
            .map_err(|err| WALError::BadRequest(err.to_string()))?;
        if selected_size + curr_size > max_chunk_size {
            break;
        }

        selected.push(pending.pop_front().unwrap());
        selected_size += curr_size;
    }

    Ok(selected)
}

fn prepare_upload_batch(batch: Vec<PendingAppend>) -> UploadBatch {
    let mut records = Vec::new();
    let mut responses = Vec::new();
    for pending in batch {
        records.extend(pending.records);
        responses.push(pending.response);
    }

    UploadBatch::new(records, responses)
}
