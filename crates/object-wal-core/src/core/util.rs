use crate::core::objectstore::{ObjectStoreError, ReadObjectStream};
use bytes::BytesMut;
use futures::stream::StreamExt;
use serde::de::DeserializeOwned;

pub const MANIFEST_KEY: &str = "manifest.json";

pub async fn stream_to_struct<T>(mut stream: ReadObjectStream) -> Result<T, ObjectStoreError>
where
    T: DeserializeOwned,
{
    let mut buf = BytesMut::new();

    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result?;
        buf.extend_from_slice(&chunk);
    }

    let object: T = serde_json::from_slice(&buf)
        .map_err(|err| ObjectStoreError::BadRequest(err.to_string()))?;

    Ok(object)
}

pub fn chunk_key(key_prefix: &str, chunk_seq: u64) -> String {
    format!("{}/chunk-{:020}", key_prefix, chunk_seq)
}

pub fn manifest_key(key_prefix: &str) -> String {
    format!("{}/{}", key_prefix, MANIFEST_KEY)
}
