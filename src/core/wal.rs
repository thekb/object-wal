use async_trait::async_trait;
use futures::Stream;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Record presents a single entry in the WAL
#[derive(Debug, Eq, PartialEq)]
pub struct Record {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Record {
    pub fn new<T: Into<Vec<u8>>>(key: T, value: T) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// Manifest presents current the state of WAL.
/// chunks < watermark_seq are garbage collected
/// writers should create a chunk with next_seq
///
#[derive(Serialize, Deserialize, Copy, Clone)]
pub struct Manifest {
    pub watermark_seq: u64,
    pub next_seq: u64,
}

#[derive(Debug, Error, Clone)]
pub enum WALError {
    #[error("internal error: {0}")]
    Internal(String),
    #[error("does not exist: {0}")]
    DoesNotExist(String),
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("manifest out of date: {0}")]
    ManifestOutOfDate(String),
}

pub struct TailRequest {
    pub start_seq: u64,
}

/// Tailer is the trait for tailing the WAL. The tailing the WAL starts from
/// start_seq. WAL tailing is automatically cancelled when the stream is dropped.
pub trait Tailer {
    fn tail(&self, cmd: TailRequest) -> impl Stream<Item = Result<Record, WALError>> + Send + '_;
}

/// AppendRequest represents a chunk of records that need to be appended to the
/// WAL. The records are all appended as a single unit. They are either appended
/// all or nothing. The append maps to a single chunk sequence number.
/// The implementations might choose to batch multiple appended
/// requests into a single chunk. In that case, multiple append requests will be
/// mapped to a single chunk sequence number.
/// The caller is acknowledged / receives response only when the records are
/// successfully persisted in the backing object store. However, the records might
/// not be visible to the tailers, until the manifest is updated.
pub struct AppendRequest {
    pub records: Vec<Record>,
}

/// AppendResponse represents the sequence number of the chunk which contains
/// the records in the append request.
pub struct AppendResponse {
    pub chunk_seq: u64,
}

/// Appends records to the WAL with an explicitly managed processing loop.
#[async_trait]
pub trait Appender {
    /// Supervise the appender's processing tasks until shutdown or failure.
    /// Run this concurrently with `append`; each instance may be run only once.
    /// Errors may include publication failures after an append was acknowledged.
    /// Dropping this future stops processing and fails queued acknowledgements.
    async fn run(&self) -> Result<(), WALError>;

    /// Queue an atomic request and wait for its durable acknowledgement.
    /// Requests submitted before `run` starts wait for it to be driven.
    async fn append(&self, cmd: AppendRequest) -> Result<AppendResponse, WALError>;
}
