use super::common::{contention, load_manifest, put_manifest, store_error};
use crate::core::{objectstore::*, wal::*};
use async_trait::async_trait;
use futures::StreamExt;
use std::sync::Arc;

/// One-shot WAL retention. Each call publishes the new watermark before
/// deleting chunks and also retries cleanup left by earlier calls.
pub struct GarbageCollectorImpl<T> {
    store: Arc<T>,
    bucket: String,
    name: String,
}

impl<T> GarbageCollectorImpl<T> {
    pub fn new(store: Arc<T>, bucket: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            store,
            bucket: bucket.into(),
            name: name.into(),
        }
    }
}

#[async_trait]
impl<T: ObjectStoreReader + ObjectStoreWriter + ObjectStoreDeleter> GarbageCollector
    for GarbageCollectorImpl<T>
{
    async fn collect(
        &self,
        cmd: GarbageCollectRequest,
    ) -> Result<GarbageCollectResponse, WALError> {
        let watermark_seq = loop {
            let info = load_manifest(self.store.as_ref(), &self.bucket, &self.name, None)
                .await?
                .ok_or_else(|| WALError::Internal("unexpected unchanged manifest".into()))?;
            if cmd.retain_from_seq > info.manifest.next_seq {
                return Err(WALError::BadRequest(
                    "retention cannot advance past the published WAL".into(),
                ));
            }
            if cmd.retain_from_seq <= info.manifest.watermark_seq {
                break info.manifest.watermark_seq;
            }
            let manifest = Manifest {
                watermark_seq: cmd.retain_from_seq,
                ..info.manifest
            };
            match put_manifest(
                self.store.as_ref(),
                &self.bucket,
                &self.name,
                manifest,
                Some(info.etag),
            )
            .await
            {
                Ok(_) => break manifest.watermark_seq,
                Err(error) if contention(&error) => tokio::task::yield_now().await,
                Err(error) => return Err(store_error(error)),
            }
        };

        let prefix = format!("{}/chunk-", self.name);
        let chunks = self.store.list_objects(ListObjectsRequest {
            bucket: self.bucket.clone(),
            prefix: Some(prefix.clone()),
        });
        futures::pin_mut!(chunks);
        let mut deleted_chunks = 0;
        while let Some(object) = chunks.next().await {
            let object = object.map_err(store_error)?;
            let Some(suffix) = object.key.strip_prefix(&prefix) else {
                continue;
            };
            if suffix.len() != 20 || !suffix.bytes().all(|byte| byte.is_ascii_digit()) {
                continue;
            }
            let Ok(seq) = suffix.parse::<u64>() else {
                continue;
            };
            if seq < watermark_seq {
                match self
                    .store
                    .delete_object(DeleteObjectRequest {
                        bucket: self.bucket.clone(),
                        key: object.key,
                    })
                    .await
                {
                    Ok(()) | Err(ObjectStoreError::NotFound(_)) => deleted_chunks += 1,
                    Err(error) => return Err(store_error(error)),
                }
            }
        }
        Ok(GarbageCollectResponse {
            watermark_seq,
            deleted_chunks,
        })
    }
}
