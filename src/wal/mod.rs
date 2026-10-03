//! Object-store implementations of the core WAL traits.
//!
//! Use a dedicated key prefix in an existing bucket. Stores must provide atomic
//! conditional writes and strongly consistent reads. Chunk objects are immutable;
//! any retention process must advance the manifest watermark before deleting them.
//!
//! `Appender::run` supervises a chunk writer and a manifest coordinator. Appends
//! acknowledge durable chunk uploads; tailer visibility follows publication.
//! The coordinator combines queued publication commands and refreshes cached
//! state every 100 ms when idle. Call `close` and await `run` to finish publication.
//! Monitor `run` for errors even after individual appends have succeeded.
//!
//! ```no_run
//! use std::sync::Arc;
//! use futures::StreamExt;
//! use object_wal::{
//!     core::{objectstore::{ObjectStoreReader, ObjectStoreWriter}, wal::*},
//!     wal::{AppenderConfig, AppenderImpl, TailerImpl},
//! };
//!
//! async fn example<T: ObjectStoreReader + ObjectStoreWriter + 'static>(store: Arc<T>) -> Result<(), WALError> {
//!     let appender = Arc::new(AppenderImpl::with_config(store.clone(), "bucket", "my-log", AppenderConfig {
//!         flush_timeout: std::time::Duration::from_millis(50),
//!     }));
//!     let runner = appender.clone();
//!     let task = tokio::spawn(async move { runner.run().await });
//!     let response = appender.append(AppendRequest {
//!         records: vec![Record::new("key", "value")],
//!     }).await;
//!     appender.close().await;
//!     task.await.map_err(|e| WALError::Internal(e.to_string()))??;
//!     let response = response?;
//!     let tailer = TailerImpl::new(store, "bucket", "my-log");
//!     let records = tailer.tail(TailRequest { start_seq: response.chunk_seq });
//!     futures::pin_mut!(records);
//!     if let Some(record) = records.next().await { let _ = record?; }
//!     Ok(())
//! }
//! ```
pub mod appender;
mod common;
mod manifest;
pub mod tailer;

pub use appender::{AppenderConfig, AppenderImpl};
pub use tailer::TailerImpl;

#[cfg(test)]
mod tests;
