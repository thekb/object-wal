use bytes::BytesMut;
use futures::StreamExt;

use crate::codec;
use crate::objectstore::ports::*;
use crate::objectstore::util::{bytes_to_stream, stream_to_struct, struct_to_stream};
use crate::types::{Chunk, Manifest, ManifestResponse, WALError};

pub const MANIFEST_KEY: &str = "manifest.json";

pub async fn get_manifest<T>(
    client: &T,
    bucket_name: &str,
    manifest_key: &str,
    etag: Option<&str>,
) -> Result<Option<ManifestResponse>, WALError>
where
    T: ObjectStoreReader,
{
    let response = client
        .get_object(GetObjectRequest {
            bucket: bucket_name.to_owned(),
            if_none_match: etag.map(String::from),
            key: manifest_key.to_string(),
        })
        .await
        .map_err(|err| WALError::Internal(err.to_string()))?;

    let Some(object_response) = response else {
        return Ok(None);
    };

    let manifest: Manifest = stream_to_struct(object_response.body)
        .await
        .map_err(|err| WALError::Internal(err.to_string()))?;

    let manifest_response = ManifestResponse {
        manifest: manifest,
        etag: object_response.etag,
    };

    Ok(Some(manifest_response))
}

pub async fn put_manifest<T>(
    client: &T,
    bucket_name: &str,
    etag: Option<&str>,
    manifest_key: &str,
    manifest: &Manifest,
) -> Result<String, WALError>
where
    T: ObjectStoreWriter,
{
    let body = struct_to_stream(manifest).map_err(|err| WALError::Internal(err.to_string()))?;
    let mut condition = PutObjectCondition::IfNoneMatch;
    if let Some(etag) = etag {
        condition = PutObjectCondition::IfMatch(etag.to_string());
    }

    let request: PubObjectRequest = PubObjectRequest {
        bucket: bucket_name.to_string(),
        key: manifest_key.to_string(),
        body: body,
        condition: Some(condition),
    };

    let response = client.put_object(request).await.map_err(|err| match err {
        ObjectStoreError::PreConditionFailed(val) | ObjectStoreError::Conflict(val) => {
            WALError::ManifestOutOfDate(val)
        }
        _ => WALError::Internal(err.to_string()),
    })?;

    Ok(response.etag)
}

pub async fn write_chunk<T>(
    client: &T,
    bucket_name: &str,
    chunk_key: &str,
    chunk: &Chunk,
) -> Result<(), WALError>
where
    T: ObjectStoreWriter,
{
    let encoded =
        codec::encode_records(&chunk.records).map_err(|err| WALError::Internal(err.to_string()))?;
    let stream = bytes_to_stream(encoded).map_err(|err| WALError::Internal(err.to_string()))?;

    let response = client
        .put_object(PubObjectRequest {
            bucket: bucket_name.to_string(),
            key: chunk_key.to_string(),
            body: stream,
            condition: None,
        })
        .await
        .map_err(|err| WALError::Internal(err.to_string()))?;

    Ok(())
}

pub async fn read_chunk<T>(
    client: &T,
    bucket_name: &str,
    chunk_key: &str,
) -> Result<Chunk, WALError>
where
    T: ObjectStoreReader,
{
    let response = client
        .get_object(GetObjectRequest {
            bucket: bucket_name.to_string(),
            if_none_match: None,
            key: chunk_key.to_string(),
        })
        .await
        .map_err(|err| WALError::Internal(err.to_string()))?;

    let Some(object_response) = response else {
        return Err(WALError::Internal(format!("expected chunk, got none")));
    };

    let mut buf = BytesMut::new();
    let mut body = object_response.body;
    while let Some(chunk_bytes_result) = body.next().await {
        let chunk_bytes = chunk_bytes_result.map_err(|err| WALError::Internal(err.to_string()))?;
        buf.extend_from_slice(&chunk_bytes);
    }

    let records = codec::decode_chunk(&buf).map_err(|err| WALError::Internal(err.to_string()))?;
    Ok(Chunk { records: records })
}
