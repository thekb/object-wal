use super::*;
use crate::core::{
    codec,
    objectstore::*,
    util::{chunk_key, manifest_key},
    wal::*,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Mutex, Notify, Semaphore};
use tokio::task::JoinHandle;

#[derive(Default)]
struct MemoryStore {
    objects: Mutex<HashMap<String, (Bytes, String)>>,
    version: AtomicUsize,
    reads: AtomicUsize,
    fail_manifest: AtomicUsize,
    conflicts: AtomicUsize,
    fail_chunk: AtomicUsize,
    phantom_collision: AtomicUsize,
    manifest_gate: Mutex<Option<Arc<Semaphore>>>,
    manifest_started: Notify,
    manifest_writes: AtomicUsize,
    chunk_gate: Mutex<Option<Arc<Semaphore>>>,
    chunk_started: Notify,
}

#[async_trait]
impl ObjectStoreReader for MemoryStore {
    async fn get_object(&self, cmd: GetObjectRequest) -> Result<GetObjectResult, ObjectStoreError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        tokio::task::yield_now().await;
        let objects = self.objects.lock().await;
        let (bytes, etag) = objects
            .get(&cmd.key)
            .ok_or_else(|| ObjectStoreError::NotFound(cmd.key.clone()))?;
        if cmd.if_none_match.as_ref() == Some(etag) {
            return Ok(GetObjectResult::NotModified);
        }
        let bytes = bytes.clone();
        Ok(GetObjectResult::Found(GetObjectResponse {
            etag: etag.clone(),
            content_length: bytes.len() as u64,
            body: Box::pin(futures::stream::iter(vec![Ok(bytes)])),
        }))
    }
    fn list_objects(
        &self,
        _: ListObjectsRequest,
    ) -> impl Stream<Item = Result<ObjectMetadata, ObjectStoreError>> {
        futures::stream::empty()
    }
}

struct ChunkDisappearsOnRead {
    inner: Arc<MemoryStore>,
    advance_watermark: bool,
    triggered: AtomicBool,
}

#[async_trait]
impl ObjectStoreReader for ChunkDisappearsOnRead {
    async fn get_object(&self, cmd: GetObjectRequest) -> Result<GetObjectResult, ObjectStoreError> {
        if cmd.key == chunk_key("wal", 0) && !self.triggered.swap(true, Ordering::SeqCst) {
            let mut objects = self.inner.objects.lock().await;
            objects.remove(&cmd.key);
            if self.advance_watermark {
                objects.insert(
                    manifest_key("wal"),
                    (
                        Bytes::from_static(br#"{"watermark_seq":1,"next_seq":2}"#),
                        "retained".into(),
                    ),
                );
            }
        }
        self.inner.get_object(cmd).await
    }

    fn list_objects(
        &self,
        cmd: ListObjectsRequest,
    ) -> impl Stream<Item = Result<ObjectMetadata, ObjectStoreError>> {
        self.inner.list_objects(cmd)
    }
}

#[async_trait]
impl ObjectStoreWriter for MemoryStore {
    async fn put_object(
        &self,
        cmd: PubObjectRequest,
    ) -> Result<PutObjectResponse, ObjectStoreError> {
        tokio::task::yield_now().await;
        if cmd.key.contains("/chunk-") {
            let gate = self.chunk_gate.lock().await.take();
            if let Some(gate) = gate {
                self.chunk_started.notify_one();
                gate.acquire_owned().await.unwrap().forget();
            }
        }
        if matches!(cmd.condition, Some(PutObjectCondition::IfMatch(_))) {
            let gate = self.manifest_gate.lock().await.clone();
            self.manifest_started.notify_one();
            if let Some(gate) = gate {
                gate.acquire_owned().await.unwrap().forget();
            }
            self.manifest_writes.fetch_add(1, Ordering::SeqCst);
        } else if cmd.key.contains("/chunk-") && self.fail_chunk.swap(0, Ordering::SeqCst) > 0 {
            return Err(ObjectStoreError::Internal("injected chunk failure".into()));
        }
        if matches!(cmd.condition, Some(PutObjectCondition::IfMatch(_)))
            && self.fail_manifest.swap(0, Ordering::SeqCst) > 0
        {
            return Err(ObjectStoreError::Internal(
                "injected manifest failure".into(),
            ));
        }
        if self
            .conflicts
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |v| v.checked_sub(1))
            .is_ok()
        {
            return Err(ObjectStoreError::Conflict("injected conflict".into()));
        }
        if cmd.key.contains("/chunk-") && self.phantom_collision.swap(0, Ordering::SeqCst) > 0 {
            return Err(ObjectStoreError::PreConditionFailed(
                "injected collision without an object".into(),
            ));
        }
        let mut objects = self.objects.lock().await;
        let existing = objects.get(&cmd.key);
        let allowed = match cmd.condition {
            Some(PutObjectCondition::IfNoneMatch) => existing.is_none(),
            Some(PutObjectCondition::IfMatch(etag)) => {
                existing.is_some_and(|(_, current)| *current == etag)
            }
            None => true,
        };
        if !allowed {
            return Err(ObjectStoreError::PreConditionFailed(cmd.key));
        }
        let WriteObjectBody::Bytes(bytes) = cmd.body else {
            panic!("expected bytes")
        };
        let etag = self.version.fetch_add(1, Ordering::SeqCst).to_string();
        objects.insert(cmd.key, (bytes, etag.clone()));
        Ok(PutObjectResponse { etag })
    }
}

fn request(value: &str) -> AppendRequest {
    AppendRequest {
        records: vec![Record::new("key", value)],
    }
}

async fn manifest(store: &MemoryStore) -> Manifest {
    common::load_manifest(store, "bucket", "wal", None)
        .await
        .unwrap()
        .unwrap()
        .manifest
}

#[tokio::test]
async fn concurrent_appends_are_unique_and_visible_in_order() {
    let store = Arc::new(MemoryStore::default());
    store.conflicts.store(3, Ordering::SeqCst);
    let mut tasks = Vec::new();
    for i in 0..20 {
        let (appender, run) = start_tracked(AppenderImpl::new(store.clone(), "bucket", "wal"));
        tasks.push(tokio::spawn(async move {
            let value = i.to_string();
            let seq = appender.append(request(&value)).await.unwrap().chunk_seq;
            appender.close().await;
            run.await.unwrap().unwrap();
            (seq, value)
        }));
    }
    let mut expected = Vec::new();
    for task in tasks {
        expected.push(task.await.unwrap());
    }
    expected.sort();
    assert_eq!(manifest(&store).await.next_seq, 20);
    let tailer = TailerImpl::new(store, "bucket", "wal");
    let stream = tailer.tail(TailRequest { start_seq: 0 });
    futures::pin_mut!(stream);
    for (i, (seq, value)) in expected.into_iter().enumerate() {
        assert_eq!(seq, i as u64);
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            Record::new("key", &value)
        );
    }
}

#[tokio::test]
async fn recovers_chunk_after_manifest_failure() {
    let store = Arc::new(MemoryStore::default());
    let (appender, run) = start_tracked(AppenderImpl::new(store.clone(), "bucket", "wal"));
    store.fail_manifest.store(1, Ordering::SeqCst);
    assert_eq!(
        appender.append(request("first")).await.unwrap().chunk_seq,
        0
    );
    assert!(run.await.unwrap().is_err());
    assert!(appender.append(request("stopped")).await.is_err());
    assert_eq!(manifest(&store).await.next_seq, 0);
    let (appender, run) = start_tracked(AppenderImpl::new(store.clone(), "bucket", "wal"));
    assert_eq!(
        appender.append(request("second")).await.unwrap().chunk_seq,
        1
    );
    appender.close().await;
    run.await.unwrap().unwrap();
    assert_eq!(manifest(&store).await.next_seq, 2);
    let tailer = TailerImpl::new(store, "bucket", "wal");
    let stream = tailer.tail(TailRequest { start_seq: 0 });
    futures::pin_mut!(stream);
    for value in ["first", "second"] {
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            Record::new("key", value)
        );
    }
}

#[tokio::test]
async fn rejects_invalid_requests_without_writing() {
    let store = Arc::new(MemoryStore::default());
    let appender = start_appender(AppenderImpl::new(store.clone(), "bucket", "wal"));
    for records in [
        vec![],
        (0..=codec::MAX_NUM_RECORDS)
            .map(|_| Record::new("", ""))
            .collect(),
        (0..=codec::MAX_CHUNK_LEN / codec::MAX_VAL_LEN)
            .map(|_| Record::new(vec![], vec![0; codec::MAX_VAL_LEN]))
            .collect(),
    ] {
        assert!(matches!(
            appender.append(AppendRequest { records }).await,
            Err(WALError::BadRequest(_))
        ));
    }
    assert!(store.objects.lock().await.is_empty());
}

#[tokio::test]
async fn tail_waits_for_future_sequence_and_drop_stops_polling() {
    let store = Arc::new(MemoryStore::default());
    let (appender, run) = start_tracked(AppenderImpl::new(store.clone(), "bucket", "wal"));
    appender.append(request("zero")).await.unwrap();
    let tailer = TailerImpl::new(store.clone(), "bucket", "wal");
    let mut stream = Box::pin(tailer.tail(TailRequest { start_seq: 2 }));
    assert!(
        tokio::time::timeout(Duration::from_millis(150), stream.next())
            .await
            .is_err()
    );
    appender.append(request("one")).await.unwrap();
    appender.append(request("two")).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        Record::new("key", "two")
    );
    drop(stream);
    appender.close().await;
    run.await.unwrap().unwrap();
    let reads = store.reads.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(reads, store.reads.load(Ordering::SeqCst));
}

#[tokio::test]
async fn tail_respects_watermark_and_reports_corrupt_chunks() {
    let store = Arc::new(MemoryStore::default());
    let (appender, run) = start_tracked(AppenderImpl::new(store.clone(), "bucket", "wal"));
    appender.append(request("zero")).await.unwrap();
    appender.append(request("one")).await.unwrap();
    appender.close().await;
    run.await.unwrap().unwrap();
    {
        let mut objects = store.objects.lock().await;
        objects.insert(
            manifest_key("wal"),
            (
                Bytes::from_static(br#"{"watermark_seq":1,"next_seq":2}"#),
                "new".into(),
            ),
        );
        objects.remove(&chunk_key("wal", 0));
    }
    let tailer = TailerImpl::new(store.clone(), "bucket", "wal");
    let mut stream = Box::pin(tailer.tail(TailRequest { start_seq: 0 }));
    assert_eq!(
        stream.next().await.unwrap().unwrap(),
        Record::new("key", "one")
    );
    let mut objects = store.objects.lock().await;
    let mut corrupt = objects[&chunk_key("wal", 1)].0.to_vec();
    corrupt[codec::CHUNK_HEADER_LEN + 4 + 3 + 4] ^= 1;
    objects.insert(chunk_key("wal", 1), (corrupt.into(), "bad".into()));
    drop(objects);
    let mut stream = Box::pin(tailer.tail(TailRequest { start_seq: 1 }));
    assert!(matches!(
        stream.next().await,
        Some(Err(WALError::Internal(ref error))) if error.contains("checksum")
    ));
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn tail_skips_chunk_deleted_after_manifest_read_when_retention_advances() {
    let inner = Arc::new(MemoryStore::default());
    {
        let mut objects = inner.objects.lock().await;
        objects.insert(
            manifest_key("wal"),
            (
                Bytes::from_static(br#"{"watermark_seq":0,"next_seq":2}"#),
                "original".into(),
            ),
        );
        for (seq, value) in [(0, "old"), (1, "retained")] {
            objects.insert(
                chunk_key("wal", seq),
                (
                    codec::encode_records(&request(value).records)
                        .unwrap()
                        .into(),
                    seq.to_string(),
                ),
            );
        }
    }
    let store = Arc::new(ChunkDisappearsOnRead {
        inner,
        advance_watermark: true,
        triggered: AtomicBool::new(false),
    });
    let tailer = TailerImpl::new(store.clone(), "bucket", "wal");
    let mut stream = Box::pin(tailer.tail(TailRequest { start_seq: 0 }));
    assert_eq!(
        stream.next().await.unwrap().unwrap(),
        Record::new("key", "retained")
    );
    assert!(store.triggered.load(Ordering::SeqCst));
}

#[tokio::test]
async fn tail_reports_missing_chunk_still_in_manifest() {
    let inner = Arc::new(MemoryStore::default());
    {
        let mut objects = inner.objects.lock().await;
        objects.insert(
            manifest_key("wal"),
            (
                Bytes::from_static(br#"{"watermark_seq":0,"next_seq":1}"#),
                "original".into(),
            ),
        );
        objects.insert(
            chunk_key("wal", 0),
            (
                codec::encode_records(&request("old").records)
                    .unwrap()
                    .into(),
                "chunk".into(),
            ),
        );
    }
    let store = Arc::new(ChunkDisappearsOnRead {
        inner,
        advance_watermark: false,
        triggered: AtomicBool::new(false),
    });
    let tailer = TailerImpl::new(store, "bucket", "wal");
    let mut stream = Box::pin(tailer.tail(TailRequest { start_seq: 0 }));
    assert!(matches!(
        stream.next().await,
        Some(Err(WALError::DoesNotExist(_)))
    ));
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn missing_manifest_is_a_stream_error() {
    let tailer = TailerImpl::new(Arc::new(MemoryStore::default()), "bucket", "wal");
    let mut stream = Box::pin(tailer.tail(TailRequest { start_seq: 0 }));
    assert!(matches!(
        stream.next().await,
        Some(Err(WALError::DoesNotExist(_)))
    ));
    assert!(stream.next().await.is_none());
}

#[test]
fn encoded_size_includes_framing_and_limits_are_enforced() {
    let records = request("value").records;
    let bytes = codec::encode_records(&records).unwrap();
    assert_eq!(codec::chunk_size(&records).unwrap(), bytes.len());
    assert!(codec::decode_chunk(&vec![0; codec::MAX_CHUNK_LEN + 1]).is_err());
}

fn buffered_appender(store: Arc<MemoryStore>) -> Arc<AppenderImpl<MemoryStore>> {
    start_appender(AppenderImpl::with_config(
        store,
        "bucket",
        "wal",
        AppenderConfig {
            flush_timeout: Duration::from_secs(10),
        },
    ))
}

async fn stored_records(store: &MemoryStore, seq: u64) -> Vec<Record> {
    let objects = store.objects.lock().await;
    codec::decode_chunk(&objects[&chunk_key("wal", seq)].0).unwrap()
}

#[tokio::test(start_paused = true)]
async fn buffer_timeout_starts_with_first_request_and_shares_acknowledgements() {
    let store = Arc::new(MemoryStore::default());
    let appender = buffered_appender(store.clone());
    let first = appender.append(request("first"));
    futures::pin_mut!(first);
    assert!(futures::poll!(&mut first).is_pending());
    tokio::time::advance(Duration::from_secs(6)).await;
    let second = appender.append(request("second"));
    futures::pin_mut!(second);
    assert!(futures::poll!(&mut second).is_pending());
    tokio::time::advance(Duration::from_secs(3)).await;
    assert!(store.objects.lock().await.is_empty());
    assert!(futures::poll!(&mut first).is_pending());
    assert!(futures::poll!(&mut second).is_pending());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(first.await.unwrap().chunk_seq, 0);
    assert_eq!(second.await.unwrap().chunk_seq, 0);
    assert_eq!(
        stored_records(&store, 0).await,
        vec![Record::new("key", "first"), Record::new("key", "second")]
    );
    wait_published(&store, 1).await;
}

#[tokio::test(start_paused = true)]
async fn requests_expired_during_upload_share_the_next_chunk() {
    let store = Arc::new(MemoryStore::default());
    let gate = Arc::new(Semaphore::new(0));
    *store.chunk_gate.lock().await = Some(gate.clone());
    let appender = buffered_appender(store.clone());
    let first = appender.append(request("first"));
    futures::pin_mut!(first);
    assert!(futures::poll!(&mut first).is_pending());
    store.chunk_started.notified().await;

    let mut queued: Vec<_> = ["second", "third", "fourth"]
        .into_iter()
        .map(|value| Box::pin(appender.append(request(value))))
        .collect();
    for append in &mut queued {
        assert!(futures::poll!(append.as_mut()).is_pending());
    }
    tokio::time::advance(Duration::from_secs(11)).await;
    gate.add_permits(1);
    assert_eq!(first.await.unwrap().chunk_seq, 0);
    // Expired work must finish without another buffering interval.
    tokio::time::timeout(Duration::from_secs(1), async {
        for append in queued {
            assert_eq!(append.await.unwrap().chunk_seq, 1);
        }
    })
    .await
    .unwrap();
    assert_eq!(
        stored_records(&store, 1).await,
        vec![
            Record::new("key", "second"),
            Record::new("key", "third"),
            Record::new("key", "fourth"),
        ]
    );
    appender.close().await;
    wait_published(&store, 2).await;
}

#[tokio::test(start_paused = true)]
async fn expired_queue_drain_preserves_limits_and_request_order() {
    let store = Arc::new(MemoryStore::default());
    let gate = Arc::new(Semaphore::new(0));
    *store.chunk_gate.lock().await = Some(gate.clone());
    let appender = buffered_appender(store.clone());
    let first = appender.append(request("first"));
    futures::pin_mut!(first);
    assert!(futures::poll!(&mut first).is_pending());
    store.chunk_started.notified().await;
    let large = appender.append(AppendRequest {
        records: (0..codec::MAX_NUM_RECORDS - 1)
            .map(|_| Record::new("", ""))
            .collect(),
    });
    let next = appender.append(AppendRequest {
        records: vec![Record::new("key", "second"), Record::new("key", "third")],
    });
    let last = appender.append(request("fourth"));
    futures::pin_mut!(large, next, last);
    assert!(futures::poll!(&mut large).is_pending());
    assert!(futures::poll!(&mut next).is_pending());
    assert!(futures::poll!(&mut last).is_pending());
    tokio::time::advance(Duration::from_secs(11)).await;
    gate.add_permits(1);
    assert_eq!(first.await.unwrap().chunk_seq, 0);
    assert_eq!(large.await.unwrap().chunk_seq, 1);
    assert_eq!(next.await.unwrap().chunk_seq, 2);
    assert_eq!(last.await.unwrap().chunk_seq, 2);
    assert_eq!(
        stored_records(&store, 1).await.len(),
        codec::MAX_NUM_RECORDS - 1
    );
    assert_eq!(
        stored_records(&store, 2).await,
        vec![
            Record::new("key", "second"),
            Record::new("key", "third"),
            Record::new("key", "fourth"),
        ]
    );
    appender.close().await;
    wait_published(&store, 3).await;
}

#[tokio::test(start_paused = true)]
async fn full_chunk_flushes_before_timeout() {
    let store = Arc::new(MemoryStore::default());
    let appender = buffered_appender(store.clone());
    let count = codec::MAX_KEY_VALUE_LEN / codec::MAX_VAL_LEN - 1;
    let first = appender.append(AppendRequest {
        records: (0..count)
            .map(|_| Record::new(vec![], vec![0; codec::MAX_VAL_LEN]))
            .collect(),
    });
    futures::pin_mut!(first);
    assert!(futures::poll!(&mut first).is_pending());
    let last_size = codec::MAX_KEY_VALUE_LEN - count * codec::MAX_VAL_LEN;
    let second = appender.append(AppendRequest {
        records: vec![Record::new(vec![], vec![1; last_size])],
    });
    futures::pin_mut!(second);
    assert!(futures::poll!(&mut second).is_pending());
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), first)
            .await
            .unwrap()
            .unwrap()
            .chunk_seq,
        0
    );
    assert_eq!(second.await.unwrap().chunk_seq, 0);
    let objects = store.objects.lock().await;
    assert_eq!(
        objects[&chunk_key("wal", 0)].0.len(),
        codec::MAX_KEY_VALUE_LEN + (count + 1) * 8 + codec::CHUNK_OVERHEAD_LEN
    );
    assert_eq!(
        objects[&chunk_key("wal", 0)].0.len() - codec::CHUNK_OVERHEAD_LEN - (count + 1) * 8,
        codec::MAX_KEY_VALUE_LEN
    );
}

#[tokio::test(start_paused = true)]
async fn request_that_wont_fit_starts_a_new_chunk_without_splitting() {
    let store = Arc::new(MemoryStore::default());
    let appender = buffered_appender(store.clone());
    let count = codec::MAX_KEY_VALUE_LEN / codec::MAX_VAL_LEN - 1;
    let first = appender.append(AppendRequest {
        records: (0..count)
            .map(|_| Record::new(vec![], vec![0; codec::MAX_VAL_LEN]))
            .collect(),
    });
    futures::pin_mut!(first);
    assert!(futures::poll!(&mut first).is_pending());
    let second = appender.append(AppendRequest {
        records: vec![
            Record::new(vec![], vec![1; codec::MAX_VAL_LEN]),
            Record::new(vec![], vec![2]),
        ],
    });
    futures::pin_mut!(second);
    assert!(futures::poll!(&mut second).is_pending());
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), first)
            .await
            .unwrap()
            .unwrap()
            .chunk_seq,
        0
    );
    assert!(futures::poll!(&mut second).is_pending());
    assert_eq!(second.await.unwrap().chunk_seq, 1);
    assert_eq!(stored_records(&store, 0).await.len(), count);
    assert_eq!(stored_records(&store, 1).await.len(), 2);
}

#[tokio::test(start_paused = true)]
async fn full_record_count_flushes_before_timeout() {
    let store = Arc::new(MemoryStore::default());
    let appender = buffered_appender(store.clone());
    let first = appender.append(AppendRequest {
        records: (0..codec::MAX_NUM_RECORDS - 1)
            .map(|_| Record::new("", ""))
            .collect(),
    });
    futures::pin_mut!(first);
    assert!(futures::poll!(&mut first).is_pending());
    let second = appender.append(request("last"));
    futures::pin_mut!(second);
    assert!(futures::poll!(&mut second).is_pending());
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), first)
            .await
            .unwrap()
            .unwrap()
            .chunk_seq,
        0
    );
    assert_eq!(second.await.unwrap().chunk_seq, 0);
    assert_eq!(
        stored_records(&store, 0).await.len(),
        codec::MAX_NUM_RECORDS
    );
}

#[tokio::test(start_paused = true)]
async fn chunk_failure_reaches_every_caller_and_stops_run() {
    let store = Arc::new(MemoryStore::default());
    let (appender, run) = start_tracked(AppenderImpl::new(store.clone(), "bucket", "wal"));
    store.fail_chunk.store(1, Ordering::SeqCst);
    let (first, second) = tokio::join!(
        appender.append(request("first")),
        appender.append(request("second"))
    );
    assert!(first.is_err());
    assert!(second.is_err());
    assert!(run.await.unwrap().is_err());
    assert!(
        !store
            .objects
            .lock()
            .await
            .contains_key(&chunk_key("wal", 0))
    );
    assert!(appender.append(request("third")).await.is_err());
}

#[tokio::test(start_paused = true)]
async fn cancelled_caller_and_closed_appender_drain_accepted_records() {
    let store = Arc::new(MemoryStore::default());
    let appender = buffered_appender(store.clone());
    let mut append = Box::pin(appender.append(request("accepted")));
    assert!(futures::poll!(&mut append).is_pending());
    drop(append);
    appender.close().await;
    // A bounded timeout ensures closure flushes immediately, before 10 seconds.
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Ok(Some(info)) =
                common::load_manifest(store.as_ref(), "bucket", "wal", None).await
            {
                if info.manifest.next_seq == 1 {
                    break;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        stored_records(&store, 0).await,
        vec![Record::new("key", "accepted")]
    );
}

#[tokio::test(start_paused = true)]
async fn record_count_overflow_preserves_request_boundaries() {
    let store = Arc::new(MemoryStore::default());
    let appender = buffered_appender(store.clone());
    let first = appender.append(AppendRequest {
        records: (0..codec::MAX_NUM_RECORDS - 1)
            .map(|_| Record::new("", ""))
            .collect(),
    });
    futures::pin_mut!(first);
    assert!(futures::poll!(&mut first).is_pending());
    let second = appender.append(AppendRequest {
        records: vec![Record::new("a", "1"), Record::new("b", "2")],
    });
    futures::pin_mut!(second);
    assert!(futures::poll!(&mut second).is_pending());
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), first)
            .await
            .unwrap()
            .unwrap()
            .chunk_seq,
        0
    );
    assert!(futures::poll!(&mut second).is_pending());
    assert_eq!(second.await.unwrap().chunk_seq, 1);
    assert_eq!(
        stored_records(&store, 0).await.len(),
        codec::MAX_NUM_RECORDS - 1
    );
    assert_eq!(stored_records(&store, 1).await.len(), 2);
}

#[tokio::test(start_paused = true)]
async fn zero_timeout_flushes_immediately() {
    let store = Arc::new(MemoryStore::default());
    let appender = start_appender(AppenderImpl::with_config(
        store,
        "bucket",
        "wal",
        AppenderConfig {
            flush_timeout: Duration::ZERO,
        },
    ));
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(1), appender.append(request("now")))
            .await
            .unwrap()
            .unwrap()
            .chunk_seq,
        0
    );
}

// Tests explicitly start the processing loop, just as an application must.
fn start_appender(appender: AppenderImpl<MemoryStore>) -> Arc<AppenderImpl<MemoryStore>> {
    start_tracked(appender).0
}

fn start_tracked(
    appender: AppenderImpl<MemoryStore>,
) -> (
    Arc<AppenderImpl<MemoryStore>>,
    JoinHandle<Result<(), WALError>>,
) {
    let appender = Arc::new(appender);
    let runner = appender.clone();
    let run = tokio::spawn(async move { runner.run().await });
    (appender, run)
}

async fn wait_published(store: &MemoryStore, next: u64) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if manifest(store).await.next_seq >= next {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(start_paused = true)]
async fn append_does_not_start_processing_and_run_drains_on_close() {
    let store = Arc::new(MemoryStore::default());
    let appender = AppenderImpl::new(store.clone(), "bucket", "wal");
    let mut append = Box::pin(appender.append(request("queued")));
    assert!(futures::poll!(&mut append).is_pending());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(store.objects.lock().await.is_empty());
    assert!(futures::poll!(&mut append).is_pending());
    appender.close().await;
    let (run, result) = tokio::join!(appender.run(), append);
    run.unwrap();
    assert_eq!(result.unwrap().chunk_seq, 0);
    assert_eq!(
        stored_records(&store, 0).await,
        vec![Record::new("key", "queued")]
    );
    assert!(appender.append(request("closed")).await.is_err());
    assert!(matches!(appender.run().await, Err(WALError::BadRequest(_))));
}

#[tokio::test(start_paused = true)]
async fn run_is_exclusive_and_cancellation_fails_pending_calls() {
    let appender = AppenderImpl::new(Arc::new(MemoryStore::default()), "bucket", "wal");
    let mut run = Box::pin(appender.run());
    assert!(futures::poll!(&mut run).is_pending());
    assert!(matches!(appender.run().await, Err(WALError::BadRequest(_))));
    let mut append = Box::pin(appender.append(request("buffered")));
    assert!(futures::poll!(&mut append).is_pending());
    assert!(futures::poll!(&mut run).is_pending());
    let mut queued = Box::pin(appender.append(request("queued")));
    assert!(futures::poll!(&mut queued).is_pending());
    drop(run);
    assert!(append.await.is_err());
    assert!(queued.await.is_err());
    assert!(appender.append(request("after stop")).await.is_err());
    assert!(matches!(appender.run().await, Err(WALError::BadRequest(_))));
}

#[tokio::test(start_paused = true)]
async fn uploads_continue_while_publication_is_blocked_and_close_waits_for_it() {
    let store = Arc::new(MemoryStore::default());
    let gate = Arc::new(Semaphore::new(0));
    *store.manifest_gate.lock().await = Some(gate.clone());
    let (appender, mut run) = start_tracked(AppenderImpl::with_config(
        store.clone(),
        "bucket",
        "wal",
        AppenderConfig {
            flush_timeout: Duration::ZERO,
        },
    ));
    assert_eq!(
        appender.append(request("first")).await.unwrap().chunk_seq,
        0
    );
    store.manifest_started.notified().await;
    assert_eq!(manifest(&store).await.next_seq, 0);
    for (seq, value) in [(1, "second"), (2, "third")] {
        assert_eq!(
            appender.append(request(value)).await.unwrap().chunk_seq,
            seq
        );
        assert_eq!(
            stored_records(&store, seq).await,
            vec![Record::new("key", value)]
        );
    }
    appender.close().await;
    assert!(
        tokio::time::timeout(Duration::from_secs(1), &mut run)
            .await
            .is_err()
    );
    gate.add_permits(3);
    run.await.unwrap().unwrap();
    assert_eq!(manifest(&store).await.next_seq, 3);
    // The two queued publication requests were coalesced after the first write.
    assert_eq!(store.manifest_writes.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn manifest_cas_retry_preserves_concurrent_watermark() {
    let store = Arc::new(MemoryStore::default());
    let gate = Arc::new(Semaphore::new(0));
    *store.manifest_gate.lock().await = Some(gate.clone());
    let (appender, run) = start_tracked(AppenderImpl::with_config(
        store.clone(),
        "bucket",
        "wal",
        AppenderConfig {
            flush_timeout: Duration::ZERO,
        },
    ));
    appender.append(request("first")).await.unwrap();
    store.manifest_started.notified().await;
    appender.append(request("second")).await.unwrap();
    // Another publisher advances the manifest and retention while our cached
    // ETag is stale. Retrying must preserve that watermark and advance to 2.
    store.objects.lock().await.insert(
        manifest_key("wal"),
        (
            Bytes::from_static(br#"{"watermark_seq":1,"next_seq":1}"#),
            "concurrent".into(),
        ),
    );
    appender.close().await;
    gate.add_permits(4);
    run.await.unwrap().unwrap();
    let result = manifest(&store).await;
    assert_eq!(result.watermark_seq, 1);
    assert_eq!(result.next_seq, 2);
}

#[tokio::test(start_paused = true)]
async fn cancelling_run_aborts_in_flight_manifest_publication() {
    let store = Arc::new(MemoryStore::default());
    let gate = Arc::new(Semaphore::new(0));
    *store.manifest_gate.lock().await = Some(gate.clone());
    let (appender, run) = start_tracked(AppenderImpl::with_config(
        store.clone(),
        "bucket",
        "wal",
        AppenderConfig {
            flush_timeout: Duration::ZERO,
        },
    ));
    appender.append(request("durable")).await.unwrap();
    store.manifest_started.notified().await;
    run.abort();
    assert!(run.await.unwrap_err().is_cancelled());
    // Ensure child aborts are processed before unblocking the object store.
    tokio::task::yield_now().await;
    assert!(appender.append(request("stopped")).await.is_err());
    gate.add_permits(10);
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(manifest(&store).await.next_seq, 0);
    assert_eq!(store.manifest_writes.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn publication_channel_applies_backpressure() {
    let store = Arc::new(MemoryStore::default());
    let gate = Arc::new(Semaphore::new(0));
    *store.manifest_gate.lock().await = Some(gate.clone());
    let (appender, run) = start_tracked(AppenderImpl::with_config(
        store.clone(),
        "bucket",
        "wal",
        AppenderConfig {
            flush_timeout: Duration::ZERO,
        },
    ));
    appender.append(request("first")).await.unwrap();
    store.manifest_started.notified().await;
    for seq in 1..=128 {
        assert_eq!(
            appender.append(request("queued")).await.unwrap().chunk_seq,
            seq
        );
    }
    let mut blocked = Box::pin(appender.append(request("backpressure")));
    assert!(
        tokio::time::timeout(Duration::from_secs(1), &mut blocked)
            .await
            .is_err()
    );
    assert_eq!(manifest(&store).await.next_seq, 0);
    gate.add_permits(200);
    assert_eq!(blocked.await.unwrap().chunk_seq, 129);
    appender.close().await;
    run.await.unwrap().unwrap();
    assert_eq!(manifest(&store).await.next_seq, 130);
}

#[tokio::test(start_paused = true)]
async fn publication_failure_cancels_buffered_requests() {
    let store = Arc::new(MemoryStore::default());
    let gate = Arc::new(Semaphore::new(0));
    *store.manifest_gate.lock().await = Some(gate.clone());
    let (appender, run) = start_tracked(AppenderImpl::with_config(
        store.clone(),
        "bucket",
        "wal",
        AppenderConfig {
            flush_timeout: Duration::from_secs(10),
        },
    ));
    appender.append(request("durable")).await.unwrap();
    store.manifest_started.notified().await;
    let mut pending = Box::pin(appender.append(request("buffered")));
    assert!(futures::poll!(&mut pending).is_pending());
    store.fail_manifest.store(1, Ordering::SeqCst);
    gate.add_permits(1);
    assert!(run.await.unwrap().is_err());
    assert!(pending.await.is_err());
    assert!(
        !store
            .objects
            .lock()
            .await
            .contains_key(&chunk_key("wal", 1))
    );
    assert_eq!(manifest(&store).await.next_seq, 0);
}

#[tokio::test(start_paused = true)]
async fn collision_without_a_confirmed_chunk_does_not_publish_a_gap() {
    let store = Arc::new(MemoryStore::default());
    store.phantom_collision.store(1, Ordering::SeqCst);
    let (appender, run) = start_tracked(AppenderImpl::new(store.clone(), "bucket", "wal"));
    assert!(appender.append(request("missing")).await.is_err());
    assert!(run.await.unwrap().is_err());
    assert_eq!(manifest(&store).await.next_seq, 0);
    assert!(
        !store
            .objects
            .lock()
            .await
            .contains_key(&chunk_key("wal", 1))
    );
}

#[tokio::test(start_paused = true)]
async fn coordinator_refreshes_external_changes_into_watch_state() {
    use super::manifest::{ManifestCommand, ManifestCoordinator};
    let store = Arc::new(MemoryStore::default());
    let (commands, receiver) = tokio::sync::mpsc::channel(8);
    let (state_tx, mut state) = tokio::sync::watch::channel(None);
    let coordinator =
        ManifestCoordinator::new(store.clone(), "bucket".into(), "wal".into(), state_tx);
    let run = tokio::spawn(coordinator.run(receiver));
    commands.send(ManifestCommand::Refresh).await.unwrap();
    state.wait_for(|m| m.is_some()).await.unwrap();
    store.objects.lock().await.insert(
        manifest_key("wal"),
        (
            Bytes::from_static(br#"{"watermark_seq":2,"next_seq":4}"#),
            "external".into(),
        ),
    );
    let refreshed = tokio::time::timeout(
        Duration::from_secs(1),
        state.wait_for(|m| m.is_some_and(|m| m.next_seq == 4)),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(refreshed.watermark_seq, 2);
    drop(commands);
    run.await.unwrap().unwrap();
}
