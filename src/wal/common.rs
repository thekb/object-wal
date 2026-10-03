use crate::core::{
    objectstore::*,
    util::{manifest_key, stream_to_struct},
    wal::*,
};

pub(super) struct ManifestInfo {
    pub manifest: Manifest,
    pub etag: String,
}

pub(super) fn store_error(e: ObjectStoreError) -> WALError {
    match e {
        ObjectStoreError::NotFound(s) => WALError::DoesNotExist(s),
        ObjectStoreError::BadRequest(s) => WALError::BadRequest(s),
        e => WALError::Internal(e.to_string()),
    }
}

pub(super) fn contention(e: &ObjectStoreError) -> bool {
    matches!(
        e,
        ObjectStoreError::PreConditionFailed(_)
            | ObjectStoreError::ObjectAlreadyExists(_)
            | ObjectStoreError::Conflict(_)
    )
}

pub(super) async fn load_manifest<T: ObjectStoreReader>(
    store: &T,
    bucket: &str,
    name: &str,
    etag: Option<String>,
) -> Result<Option<ManifestInfo>, WALError> {
    match store
        .get_object(GetObjectRequest {
            bucket: bucket.into(),
            key: manifest_key(name),
            if_none_match: etag,
        })
        .await
        .map_err(store_error)?
    {
        GetObjectResult::NotModified => Ok(None),
        GetObjectResult::Found(response) => {
            let manifest: Manifest = stream_to_struct(response.body).await.map_err(store_error)?;
            if manifest.watermark_seq > manifest.next_seq {
                return Err(WALError::Internal("invalid manifest watermark".into()));
            }
            Ok(Some(ManifestInfo {
                manifest,
                etag: response.etag,
            }))
        }
    }
}

pub(super) async fn put_manifest<T: ObjectStoreWriter>(
    store: &T,
    bucket: &str,
    name: &str,
    manifest: Manifest,
    etag: Option<String>,
) -> Result<String, ObjectStoreError> {
    let bytes =
        serde_json::to_vec(&manifest).map_err(|e| ObjectStoreError::Internal(e.to_string()))?;
    let response = store
        .put_object(PubObjectRequest {
            bucket: bucket.into(),
            key: manifest_key(name),
            body: WriteObjectBody::Bytes(bytes.into()),
            condition: Some(
                etag.map(PutObjectCondition::IfMatch)
                    .unwrap_or(PutObjectCondition::IfNoneMatch),
            ),
        })
        .await?;
    Ok(response.etag)
}
