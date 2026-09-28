use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use thiserror::Error;

pub const MIN_BUFFER_SIZE: usize = 256 * 1024;

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

/// Chunk is a collection of records.
/// It is used for batching multiple records, to reduce writes to the Object
/// Store.
#[derive(Eq, Debug, PartialEq)]
pub struct Chunk {
    pub records: Vec<Record>,
}

/// Snapshot represents a committed chunk to the WAL. Only the chunks whose seq num
/// is <= current snapshot are visible to readers.
#[derive(Serialize, Deserialize)]
pub struct Snapshot {
    pub seq_num: u64,
    pub chunk_path: String,
}

/// Manifest presents current the state of WAL.
/// chunks < watermark_seq are garbage collected
/// writers should create a chunk with next_seq
///
#[derive(Serialize, Deserialize)]
pub struct Manifest {
    pub watermark_seq: u64,
    pub curr_seq: u64,
    // pub current_snapshot: Option<Snapshot>,
}

pub struct AppendRequest {
    pub records: Vec<Record>,
}

pub struct AppendResponse {
    pub chunk_path: Option<String>,
}

#[derive(Debug, Error)]
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

pub struct ManifestResponse {
    pub manifest: Manifest,
    pub etag: String,
    // pub last_modified: DateTime<Utc>,
}

pub struct TailRequest {
    pub start_seq: u64,
}

pub type RecordStream = Pin<Box<dyn Stream<Item = Result<Record, WALError>> + Send + Sync>>;

pub struct GarbageCollectRequest {
    pub until_seq: u64,
}

pub struct GarbageCollectResponse {
    pub num_deleted_chunks: u64,
}

#[async_trait]
pub trait WALWriter {
    async fn append(&self, cmd: AppendRequest) -> Result<AppendResponse, WALError>;
}

#[async_trait]

pub trait WALTailer {
    async fn tail(&mut self, cmd: TailRequest) -> Result<RecordStream, WALError>;
}

#[async_trait]

pub trait WALGarbageCollector {
    async fn garbage_collect(
        &self,
        cmd: GarbageCollectRequest,
    ) -> Result<GarbageCollectResponse, WALError>;
}
