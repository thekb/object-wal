use super::ports::{ObjectStoreError, ObjectStream};
use bytes::{Bytes, BytesMut};
use futures::{
    StreamExt,
    stream::{self},
};
use serde::{Serialize, de::DeserializeOwned};
use std::boxed::Box;

pub const CHUNK_SIZE: usize = 64 * 1024;

pub fn struct_to_stream<T>(data: &T) -> Result<ObjectStream, ObjectStoreError>
where
    T: Serialize,
{
    let json_bytes = serde_json::to_vec(data).map_err(|err| {
        ObjectStoreError::BadRequest(format!("unable to serialize to json: {}", err.to_string()))
    })?;

    let payload = Bytes::from(json_bytes);
    let stream = stream::once(async move { Ok(payload) });

    Ok(Box::pin(stream))
}

pub fn bytes_to_stream(data: Vec<u8>) -> Result<ObjectStream, ObjectStoreError> {
    let buf = Bytes::from(data);
    let mut chunks = Vec::new();

    let mut position = 0;
    while position < buf.len() {
        let end = std::cmp::min(position + CHUNK_SIZE, buf.len());
        let chunk = buf.slice(position..end);
        chunks.push(Ok(chunk));
        position = end;
    }

    Ok(Box::pin(stream::iter(chunks)))
}

pub async fn stream_to_struct<T>(mut stream: ObjectStream) -> Result<T, ObjectStoreError>
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
