use super::common::{load_manifest, store_error};
use crate::core::{codec, objectstore::*, util::chunk_key, wal::*};
use futures::{Stream, StreamExt};
use std::{sync::Arc, time::Duration};

/// A lazy tailer. Each stream polls independently and stops on its first error.
/// Requests below the retention watermark begin at the oldest retained chunk.
pub struct TailerImpl<T> {
    store: Arc<T>,
    bucket: String,
    name: String,
}

impl<T> TailerImpl<T> {
    pub fn new(store: Arc<T>, bucket: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            store,
            bucket: bucket.into(),
            name: name.into(),
        }
    }
}

impl<T: ObjectStoreReader> Tailer for TailerImpl<T> {
    fn tail(&self, cmd: TailRequest) -> impl Stream<Item = Result<Record, WALError>> + Send + '_ {
        async_stream::try_stream! {
            let mut seq = cmd.start_seq;
            let mut etag = None;
            loop {
                if let Some(info) = load_manifest(self.store.as_ref(), &self.bucket, &self.name, etag.clone()).await? {
                    etag = Some(info.etag);
                    seq = seq.max(info.manifest.watermark_seq);
                    let mut next_seq = info.manifest.next_seq;
                    while seq < next_seq {
                        let result = match self.store.get_object(GetObjectRequest {
                            bucket: self.bucket.clone(), key: chunk_key(&self.name, seq), if_none_match: None,
                        }).await {
                            Ok(result) => result,
                            Err(error @ ObjectStoreError::NotFound(_)) => {
                                // Retention may have advanced since the manifest snapshot.
                                if let Some(fresh) = load_manifest(self.store.as_ref(), &self.bucket, &self.name, etag.clone()).await? {
                                    etag = Some(fresh.etag);
                                    if fresh.manifest.watermark_seq > seq {
                                        seq = fresh.manifest.watermark_seq;
                                        next_seq = fresh.manifest.next_seq;
                                        continue;
                                    }
                                }
                                Err(store_error(error))?
                            }
                            Err(error) => Err(store_error(error))?,
                        };
                        let GetObjectResult::Found(mut response) = result else {
                            Err(WALError::Internal("unexpected unchanged chunk".into()))?;
                            unreachable!();
                        };
                        let mut bytes = Vec::new();
                        while let Some(part) = response.body.next().await {
                            let part = part.map_err(store_error)?;
                            if bytes.len().saturating_add(part.len()) > codec::MAX_CHUNK_LEN {
                                Err(WALError::Internal("chunk exceeds size limit".into()))?;
                            }
                            bytes.extend_from_slice(&part);
                        }
                        let records = codec::decode_chunk(&bytes).map_err(|e| WALError::Internal(e.to_string()))?;
                        for record in records { yield record; }
                        seq += 1;
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}
