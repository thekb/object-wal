//! Opt-in tests against an existing S3-compatible bucket. See the root README.
use std::{
    error::Error,
    future::Future,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use aws_sdk_s3::config::{RequestChecksumCalculation, ResponseChecksumValidation};
use bytes::Bytes;
use futures::{FutureExt, StreamExt};
use object_wal::{
    core::{
        codec,
        objectstore::*,
        util::{chunk_key, manifest_key, stream_to_struct},
        wal::*,
    },
    objectstore::s3::S3Store,
    wal::{AppenderConfig, AppenderImpl, TailerImpl},
};
use tokio::{task::JoinSet, time::timeout};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
type Case<'a> = Pin<Box<dyn Future<Output = TestResult> + Send + 'a>>;
static RUN_ID: AtomicU64 = AtomicU64::new(0);

struct Harness {
    client: aws_sdk_s3::Client,
    store: Arc<S3Store>,
    bucket: String,
    prefix: String,
    appenders: Vec<Arc<AppenderImpl<S3Store>>>,
    tasks: JoinSet<Result<(), WALError>>,
}

impl Harness {
    async fn new(label: &str) -> TestResult<Self> {
        let bucket = required_env("S3_TEST_BUCKET")?;
        let endpoint = required_env("AWS_ENDPOINT_URL")?;
        required_env("AWS_REGION")?;
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let config = aws_sdk_s3::config::Builder::from(&config)
            .endpoint_url(endpoint)
            .force_path_style(true)
            // Keep the tests usable with S3-compatible services that do not
            // implement the SDK's optional automatic checksum algorithms.
            .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
            .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
            .timeout_config(
                aws_sdk_s3::config::timeout::TimeoutConfig::builder()
                    .operation_timeout(Duration::from_secs(15))
                    .build(),
            )
            .build();
        let client = aws_sdk_s3::Client::from_conf(config);
        let prefix = format!(
            "object-wal-integration/{label}-{}-{}-{}",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
            std::process::id(),
            RUN_ID.fetch_add(1, Ordering::Relaxed)
        );
        Ok(Self {
            store: Arc::new(S3Store::new(client.clone())),
            client,
            bucket,
            prefix,
            appenders: Vec::new(),
            tasks: JoinSet::new(),
        })
    }

    fn appender(&mut self, flush_timeout: Duration) -> Arc<AppenderImpl<S3Store>> {
        let appender = Arc::new(AppenderImpl::with_config(
            self.store.clone(),
            &self.bucket,
            &self.prefix,
            AppenderConfig { flush_timeout },
        ));
        let runner = appender.clone();
        self.tasks.spawn(async move { runner.run().await });
        self.appenders.push(appender.clone());
        appender
    }

    async fn drain(&mut self) -> TestResult {
        for appender in self.appenders.drain(..) {
            appender.close().await;
        }
        let result = timeout(Duration::from_secs(30), async {
            while let Some(result) = self.tasks.join_next().await {
                result??;
            }
            TestResult::Ok(())
        })
        .await;
        match result {
            Ok(Ok(())) => Ok(()),
            failure => {
                self.tasks.shutdown().await;
                match failure {
                    Ok(Err(e)) => Err(e),
                    Err(e) => Err(e.into()),
                    _ => unreachable!(),
                }
            }
        }
    }

    async fn manifest(&self) -> TestResult<Manifest> {
        let GetObjectResult::Found(object) = self
            .store
            .get_object(GetObjectRequest {
                bucket: self.bucket.clone(),
                key: manifest_key(&self.prefix),
                if_none_match: None,
            })
            .await?
        else {
            return Err("unexpected unchanged manifest".into());
        };
        Ok(stream_to_struct(object.body).await?)
    }

    async fn read_records(&self, start_seq: u64, count: usize) -> TestResult<Vec<Record>> {
        let tailer = TailerImpl::new(self.store.clone(), &self.bucket, &self.prefix);
        let stream = tailer.tail(TailRequest { start_seq });
        futures::pin_mut!(stream);
        let mut records = Vec::new();
        for _ in 0..count {
            records.push(stream.next().await.ok_or("tail ended early")??);
        }
        Ok(records)
    }

    async fn cleanup(&self) -> TestResult {
        // Include the slash: cleanup must never match a neighbouring test's prefix.
        let prefix = format!("{}/", self.prefix);
        let mut pages = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&prefix)
            .into_paginator()
            .send();
        let mut keys = Vec::new();
        while let Some(page) = pages.next().await {
            for object in page?.contents() {
                let key = object.key().ok_or("listed object has no key")?;
                if !key.starts_with(&prefix) {
                    return Err("cleanup received an out-of-prefix key".into());
                }
                keys.push(key.to_owned());
            }
        }
        let mut failures = Vec::new();
        for key in keys {
            if let Err(error) = self
                .client
                .delete_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
                .await
            {
                failures.push(format!("{key}: {error}"));
            }
        }
        if !failures.is_empty() {
            return Err(format!("cleanup failed: {}", failures.join(", ")).into());
        }
        Ok(())
    }
}

fn required_env(name: &str) -> TestResult<String> {
    let value = std::env::var(name)
        .map_err(|_| format!("set {name} through .envrc before running live tests"))?;
    if value.trim().is_empty() || value.contains('<') {
        return Err(format!("configure {name}; it is empty or still a placeholder").into());
    }
    Ok(value)
}

async fn run_case(
    label: &str,
    case: impl for<'a> FnOnce(&'a mut Harness) -> Case<'a>,
) -> TestResult {
    let mut harness = timeout(Duration::from_secs(30), Harness::new(label)).await??;
    let outcome = AssertUnwindSafe(timeout(Duration::from_secs(90), case(&mut harness)))
        .catch_unwind()
        .await;
    let drained = harness.drain().await;
    let cleaned = timeout(Duration::from_secs(60), harness.cleanup()).await;
    // Report cleanup failures even if the original case panicked or timed out.
    if let Err(error) = &drained {
        eprintln!("appender shutdown failed: {error}");
    }
    if !matches!(&cleaned, Ok(Ok(()))) {
        eprintln!("cleanup failed; inspect test prefix {}", harness.prefix);
    }
    match outcome {
        Err(panic) => std::panic::resume_unwind(panic),
        Ok(result) => result??,
    }
    drained?;
    cleaned??;
    Ok(())
}

fn request(value: &str) -> AppendRequest {
    AppendRequest {
        records: vec![Record::new("key", value)],
    }
}

#[tokio::test]
#[ignore = "requires R2/S3 credentials, endpoint, and an existing test bucket"]
async fn s3_conditional_operations() -> TestResult {
    run_case("conditions", |h| {
        Box::pin(async move {
            let key = format!("{}/conditional", h.prefix);
            let put = |value: &'static [u8], condition| PubObjectRequest {
                bucket: h.bucket.clone(),
                key: key.clone(),
                body: WriteObjectBody::Bytes(Bytes::from_static(value)),
                condition: Some(condition),
            };
            let initial = h
                .store
                .put_object(put(b"first", PutObjectCondition::IfNoneMatch))
                .await?;
            assert!(matches!(
                h.store
                    .put_object(put(b"duplicate", PutObjectCondition::IfNoneMatch))
                    .await,
                Err(ObjectStoreError::PreConditionFailed(_)
                    | ObjectStoreError::ObjectAlreadyExists(_))
            ));
            assert!(matches!(
                h.store
                    .get_object(GetObjectRequest {
                        bucket: h.bucket.clone(),
                        key: key.clone(),
                        if_none_match: Some(initial.etag.clone())
                    })
                    .await?,
                GetObjectResult::NotModified
            ));
            let updated = h
                .store
                .put_object(put(
                    b"second",
                    PutObjectCondition::IfMatch(initial.etag.clone()),
                ))
                .await?;
            assert_ne!(initial.etag, updated.etag);
            assert!(matches!(
                h.store
                    .put_object(put(b"stale", PutObjectCondition::IfMatch(initial.etag)))
                    .await,
                Err(ObjectStoreError::PreConditionFailed(_))
            ));
            let GetObjectResult::Found(mut object) = h
                .store
                .get_object(GetObjectRequest {
                    bucket: h.bucket.clone(),
                    key: key.clone(),
                    if_none_match: None,
                })
                .await?
            else {
                panic!("expected object");
            };
            let mut bytes = Vec::new();
            while let Some(part) = object.body.next().await {
                bytes.extend_from_slice(&part?);
            }
            assert_eq!(bytes, b"second");
            let listed = h.store.list_objects(ListObjectsRequest {
                bucket: h.bucket.clone(),
                prefix: Some(format!("{}/", h.prefix)),
            });
            futures::pin_mut!(listed);
            assert_eq!(
                listed.next().await.ok_or("expected listed object")??.key,
                key
            );
            assert!(listed.next().await.is_none());
            assert!(matches!(
                h.store
                    .get_object(GetObjectRequest {
                        bucket: h.bucket.clone(),
                        key: format!("{}/missing", h.prefix),
                        if_none_match: None
                    })
                    .await,
                Err(ObjectStoreError::NotFound(_))
            ));
            Ok(())
        })
    })
    .await
}

#[tokio::test]
#[ignore = "requires R2/S3 credentials, endpoint, and an existing test bucket"]
async fn s3_buffered_append_and_tail() -> TestResult {
    run_case("buffered", |h| {
        Box::pin(async move {
            let appender = h.appender(Duration::from_millis(100));
            let (first, second) = tokio::join!(
                appender.append(request("first")),
                appender.append(request("second"))
            );
            let seq = first?.chunk_seq;
            assert_eq!(seq, second?.chunk_seq);
            assert_eq!(seq, 0);
            assert_eq!(appender.append(request("third")).await?.chunk_seq, 1);
            h.drain().await?;
            assert_eq!(h.manifest().await?.next_seq, 2);
            assert_eq!(
                h.read_records(0, 3).await?,
                vec![
                    Record::new("key", "first"),
                    Record::new("key", "second"),
                    Record::new("key", "third")
                ]
            );
            assert_eq!(
                h.read_records(1, 1).await?,
                vec![Record::new("key", "third")]
            );
            Ok(())
        })
    })
    .await
}

#[tokio::test]
#[ignore = "requires R2/S3 credentials, endpoint, and an existing test bucket"]
async fn s3_independent_appenders() -> TestResult {
    run_case("writers", |h| {
        Box::pin(async move {
            let first = h.appender(Duration::ZERO);
            let second = h.appender(Duration::ZERO);
            let mut expected = Vec::new();
            for round in 0..3 {
                let a = format!("a-{round}");
                let b = format!("b-{round}");
                let (left, right) =
                    tokio::join!(first.append(request(&a)), second.append(request(&b)));
                expected.push((left?.chunk_seq, a));
                expected.push((right?.chunk_seq, b));
            }
            h.drain().await?;
            expected.sort();
            assert_eq!(h.manifest().await?.next_seq, 6);
            for (index, (seq, _)) in expected.iter().enumerate() {
                assert_eq!(*seq, index as u64);
            }
            let records: Vec<_> = expected
                .iter()
                .map(|(_, value)| Record::new("key", value.as_str()))
                .collect();
            assert_eq!(h.read_records(0, records.len()).await?, records);
            Ok(())
        })
    })
    .await
}

#[tokio::test]
#[ignore = "requires R2/S3 credentials, endpoint, and an existing test bucket"]
async fn s3_recovers_unpublished_chunk() -> TestResult {
    run_case("recovery", |h| {
        Box::pin(async move {
            h.store
                .put_object(PubObjectRequest {
                    bucket: h.bucket.clone(),
                    key: chunk_key(&h.prefix, 0),
                    body: WriteObjectBody::Bytes(
                        codec::encode_records(&request("orphan").records)?.into(),
                    ),
                    condition: Some(PutObjectCondition::IfNoneMatch),
                })
                .await?;
            let appender = h.appender(Duration::ZERO);
            assert_eq!(appender.append(request("new")).await?.chunk_seq, 1);
            h.drain().await?;
            assert_eq!(h.manifest().await?.next_seq, 2);
            assert_eq!(
                h.read_records(0, 2).await?,
                vec![Record::new("key", "orphan"), Record::new("key", "new")]
            );
            Ok(())
        })
    })
    .await
}
