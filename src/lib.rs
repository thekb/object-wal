//! A buffered write-ahead log backed by object storage.
//!
//! See [`wal`] for appender lifecycle, publication, and tailing examples.

pub mod core;
pub mod objectstore;
pub mod wal;
