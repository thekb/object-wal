use super::common::{ManifestInfo, contention, load_manifest, put_manifest, store_error};
use crate::core::{objectstore::*, wal::*};
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, watch},
    time::{MissedTickBehavior, interval},
};

pub(super) enum ManifestCommand {
    Refresh,
    // Only the sequential chunk writer sends this, after confirming every slot
    // from its initial manifest through chunk_seq is durable.
    PublishThrough { chunk_seq: u64 },
}

pub(super) struct ManifestCoordinator<T> {
    store: Arc<T>,
    bucket: String,
    name: String,
    state: watch::Sender<Option<Manifest>>,
    info: Option<ManifestInfo>,
}

impl<T: ObjectStoreReader + ObjectStoreWriter> ManifestCoordinator<T> {
    pub fn new(
        store: Arc<T>,
        bucket: String,
        name: String,
        state: watch::Sender<Option<Manifest>>,
    ) -> Self {
        Self {
            store,
            bucket,
            name,
            state,
            info: None,
        }
    }

    pub async fn run(
        mut self,
        mut commands: mpsc::Receiver<ManifestCommand>,
    ) -> Result<(), WALError> {
        let mut ticker = interval(Duration::from_millis(100));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            let command = tokio::select! (
                biased;
                command = commands.recv() => command,
                _ = ticker.tick(), if self.info.is_some() => {
                    self.refresh().await?;
                    continue;
                }
            );
            let Some(command) = command else {
                return Ok(());
            };
            let mut through = None;
            let mut refresh_requested = false;
            let mut absorb = |command| match command {
                ManifestCommand::Refresh => refresh_requested = true,
                ManifestCommand::PublishThrough { chunk_seq } => {
                    through = Some(through.map_or(chunk_seq, |old: u64| old.max(chunk_seq)));
                }
            };
            absorb(command);
            // Bound each coalescing pass so producers cannot starve publication.
            for _ in 0..127 {
                match commands.try_recv() {
                    Ok(command) => absorb(command),
                    Err(_) => break,
                }
            }
            if self.info.is_none() {
                self.initialize().await?;
            } else if refresh_requested {
                self.refresh().await?;
            }
            if let Some(seq) = through {
                self.publish(seq).await?;
            }
        }
    }

    async fn initialize(&mut self) -> Result<(), WALError> {
        loop {
            match load_manifest(self.store.as_ref(), &self.bucket, &self.name, None).await {
                Ok(Some(info)) => {
                    self.state.send_replace(Some(info.manifest));
                    self.info = Some(info);
                    return Ok(());
                }
                Ok(None) => return Err(WALError::Internal("unexpected unchanged manifest".into())),
                Err(WALError::DoesNotExist(_)) => {
                    let manifest = Manifest {
                        watermark_seq: 0,
                        next_seq: 0,
                    };
                    match put_manifest(
                        self.store.as_ref(),
                        &self.bucket,
                        &self.name,
                        manifest,
                        None,
                    )
                    .await
                    {
                        Ok(etag) => {
                            self.state.send_replace(Some(manifest));
                            self.info = Some(ManifestInfo { manifest, etag });
                            return Ok(());
                        }
                        Err(e) if contention(&e) => tokio::task::yield_now().await,
                        Err(e) => return Err(store_error(e)),
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn refresh(&mut self) -> Result<(), WALError> {
        let etag = self.info.as_ref().map(|info| info.etag.clone());
        if let Some(info) =
            load_manifest(self.store.as_ref(), &self.bucket, &self.name, etag).await?
        {
            self.state.send_replace(Some(info.manifest));
            self.info = Some(info);
        }
        Ok(())
    }

    async fn publish(&mut self, chunk_seq: u64) -> Result<(), WALError> {
        let next_seq = chunk_seq
            .checked_add(1)
            .ok_or_else(|| WALError::BadRequest("sequence number exhausted".into()))?;
        loop {
            let info = self.info.as_ref().unwrap();
            if info.manifest.next_seq >= next_seq {
                return Ok(());
            }
            let manifest = Manifest {
                next_seq,
                ..info.manifest
            };
            match put_manifest(
                self.store.as_ref(),
                &self.bucket,
                &self.name,
                manifest,
                Some(info.etag.clone()),
            )
            .await
            {
                Ok(etag) => {
                    self.state.send_replace(Some(manifest));
                    self.info = Some(ManifestInfo { manifest, etag });
                    return Ok(());
                }
                Err(e) if contention(&e) => {
                    // Preserve concurrent progress and retention updates on CAS retry.
                    self.refresh().await?;
                    tokio::task::yield_now().await;
                }
                Err(e) => return Err(store_error(e)),
            }
        }
    }
}
