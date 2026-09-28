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

/// Chunk is a collection of records.
/// It is used for batching multiple records, to reduce writes to the Object
/// Store. Each chunk is represented by an unique log sequence number.
#[derive(Eq, Debug, PartialEq)]
pub struct Chunk {
    pub records: Vec<Record>,
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

// pub type RecordStream = Stream<Item = Result<Record, WALError>>;

pub struct TailRequest {
    pub start_seq: u64,
}

/// WALTailer is the trait for tailing the WAL. The tailing the WAL starts from
/// start_seq. WAL tailing is automatically cancelled when the stream is dropped.
pub trait WALTailer {
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

/// WALAppender is the trait for appends records to the WAL.
#[async_trait]
pub trait Appender {
    async fn append(&self, cmd: AppendRequest) -> Result<AppendResponse, WALError>;
}
