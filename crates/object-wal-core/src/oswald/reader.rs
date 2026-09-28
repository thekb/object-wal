use super::*;
use crate::objectstore::ports::ObjectStoreReader;
use crate::types::{ManifestResponse, WALError};
use crate::{objectstore, types};
use async_trait::async_trait;
use std::boxed::Box;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, mpsc};
use tokio::time::{MissedTickBehavior, interval};
use tokio_stream::wrappers::ReceiverStream;

struct TailWorker<T>
where
    T: ObjectStoreReader,
{
    store: Arc<T>,
    bucket_name: String,
    key_prefix: String,
    record_tx: mpsc::Sender<Result<types::Record, types::WALError>>,
}

impl<T> TailWorker<T>
where
    T: ObjectStoreReader,
{
    pub fn new(
        store: Arc<T>,
        bucket_name: String,
        key_prefix: String,
        records_tx: mpsc::Sender<Result<types::Record, types::WALError>>,
    ) -> Self {
        TailWorker {
            store: store,
            bucket_name: bucket_name,
            key_prefix: key_prefix,
            record_tx: records_tx,
        }
    }

    async fn discover_and_read_chunks(&self, chunk_from: u64) -> Result<(), types::WALError> {
        // init manifest
        let manifest_response = get_manifest(
            self.store.as_ref(),
            &self.bucket_name,
            &objectstore::manifest_key(&self.key_prefix),
            None,
        )
        .await?
        .ok_or(WALError::DoesNotExist("manifest not found".to_owned()))?;

        let mut etag = manifest_response.etag;
        let mut start_seq = std::cmp::max(manifest_response.manifest.watermark_seq, chunk_from);
        let mut end_seq = manifest_response.manifest.curr_seq + 1;

        loop {
            for curr_seq in start_seq..end_seq {
                self.read_chunk(curr_seq).await?;
            }

            start_seq = end_seq;

            let refreshed_manifest_result = self.wait_for_manifest_refresh(&etag).await;
            match refreshed_manifest_result {
                Err(err) => {
                    return Err(err);
                }
                Ok(response) => {
                    etag = response.etag;
                    end_seq = response.manifest.curr_seq + 1;
                }
            }
        }
    }

    async fn wait_for_manifest_refresh(&self, etag: &str) -> Result<ManifestResponse, WALError> {
        let mut ticker = interval(Duration::from_secs(1));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            ticker.tick().await;

            let manifest_response_result = get_manifest(
                self.store.as_ref(),
                &self.bucket_name,
                &objectstore::manifest_key(&self.key_prefix),
                Some(etag),
            )
            .await;

            match manifest_response_result {
                Err(err) => {
                    return Err(err);
                }
                Ok(result) => {
                    if let Some(response) = result {
                        return Ok(response);
                    }
                }
            }
        }
    }

    async fn read_chunk(&self, chunk_seq: u64) -> Result<(), types::WALError> {
        let tx = self.record_tx.clone();

        let chunk = read_chunk(
            self.store.as_ref(),
            &self.bucket_name,
            objectstore::chunk_key(&self.key_prefix, chunk_seq).as_str(),
        )
        .await?;

        for record in chunk.records {
            tx.send(Ok(record))
                .await
                .map_err(|err| WALError::Internal(err.to_string()))?;
        }

        Ok(())
    }
}

/// WALTailer implements a WAL tailer on top of object storage using the protocol
/// describe in https://nvartolomei.com/oswald/#tailing.
pub struct WALTailer<T>
where
    T: ObjectStoreReader,
{
    store: Arc<T>,
    bucket_name: String,
    key_prefix: String,
    record_tx: Option<mpsc::Sender<Result<types::Record, types::WALError>>>,
    record_rx: Mutex<Option<mpsc::Receiver<Result<types::Record, types::WALError>>>>,
}

impl<T> WALTailer<T>
where
    T: ObjectStoreReader,
{
    pub fn new(store: Arc<T>, bucket_name: String, key_prefix: String) -> Self {
        let (tx, rx) = mpsc::channel::<Result<types::Record, types::WALError>>(8);
        WALTailer {
            store: store,
            bucket_name: bucket_name,
            key_prefix: key_prefix,
            record_tx: Some(tx),
            record_rx: Mutex::new(Some(rx)),
        }
    }
}

#[async_trait]
impl<T> types::WALTailer for WALTailer<T>
where
    T: ObjectStoreReader + 'static,
{
    async fn tail(
        &mut self,
        cmd: types::TailRequest,
    ) -> Result<types::RecordStream, types::WALError> {
        let rx = self.record_rx.lock().await.take().ok_or_else(|| {
            types::WALError::Internal(format!("only one tail allowed per tailer"))
        })?;

        // follow the protocol described in
        // https://nvartolomei.com/oswald/#initialization

        let tx = self
            .record_tx
            .take()
            .ok_or(WALError::Internal(format!("tx missing")))?;

        let worker = TailWorker::new(
            Arc::clone(&self.store),
            self.bucket_name.to_owned(),
            self.key_prefix.to_owned(),
            tx.clone(),
        );

        tokio::spawn(async move {
            let result = worker.discover_and_read_chunks(cmd.start_seq).await;
            let tx = tx.clone();
            if let Err(err) = result {
                let _ = tx.send(Err(err)).await;
            }
        });

        let stream = ReceiverStream::new(rx);

        Ok(Box::pin(stream))
    }
}
