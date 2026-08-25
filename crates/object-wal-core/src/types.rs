use chrono::{DateTime, Utc};
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
    pub next_seq: u64,
    pub current_snapshot: Option<Snapshot>,
}

pub struct AppendRequest {
    pub records: Vec<Record>,
    pub sync: bool,
}

pub struct AppendResponse {}

#[derive(Debug, Error)]
pub enum WALError {
    #[error("internal error: {0}")]
    Internal(String),
}

pub struct ManifestResponse {
    pub manifest: Manifest,
    pub etag: Option<String>,
    // pub last_modified: DateTime<Utc>,
}
