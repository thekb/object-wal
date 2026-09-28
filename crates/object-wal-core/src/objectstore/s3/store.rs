use crate::core::objectstore::*;
use async_stream::try_stream;
use async_trait::async_trait;
use aws_sdk_s3 as s3;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::{ByteStream, SdkBody};
use futures::Stream;
use futures::TryStreamExt;
use http_body::Frame;
use http_body_util::StreamBody;
use std::boxed::Box;

fn into_read_object_stream(body: aws_sdk_s3::primitives::ByteStream) -> ReadObjectStream {
    let stream = futures::stream::unfold(body, |mut body| async move {
        body.next().await.map(|chunk| {
            let chunk = chunk.map_err(|err| {
                ObjectStoreError::Internal(format!("failed to read S3 object body: {err}"))
            });
            (chunk, body)
        })
    });
    Box::pin(stream)
}

fn into_aws_byte_stream(write_body: WriteObjectBody) -> ByteStream {
    match write_body {
        WriteObjectBody::Bytes(buf) => ByteStream::from(buf),
        WriteObjectBody::Stream {
            stream,
            content_length,
        } => {
            let frames = stream.map_ok(Frame::data);
            let body = StreamBody::new(frames);
            let sdk_body = SdkBody::from_body_1_x(body);

            ByteStream::new(sdk_body)
        }
    }
}

pub struct S3Store {
    client: s3::Client,
}

impl S3Store {
    pub fn new(client: s3::Client) -> Self {
        S3Store { client }
    }
}

#[async_trait]
impl ObjectStoreReader for S3Store {
    async fn get_object(&self, cmd: GetObjectRequest) -> Result<GetObjectResult, ObjectStoreError> {
        let client = self.client.clone();
        let mut request = client.get_object().bucket(cmd.bucket).key(cmd.key);

        if let Some(etag) = cmd.if_none_match {
            request = request.if_none_match(etag);
        }
        let result = request.send().await;

        match result {
            Ok(response) => {
                let etag = response.e_tag.ok_or(ObjectStoreError::Internal(
                    "etag does not exist for object".to_owned(),
                ))?;

                let content_length = response
                    .content_length
                    .and_then(|size| u64::try_from(size).ok())
                    .ok_or_else(|| {
                        ObjectStoreError::Internal("S3 returned an invalid object size".to_owned())
                    })?;

                let body = into_read_object_stream(response.body);
                return Ok(GetObjectResult::Found(GetObjectResponse {
                    body,
                    etag,
                    content_length,
                }));
            }
            Err(err) => {
                if let Some(se) = err.as_service_error() {
                    if se.is_no_such_key() {
                        return Err(ObjectStoreError::NotFound(err.to_string()));
                    }
                }
                let status = err.raw_response().map(|r| r.status().as_u16());
                match status {
                    Some(304) => {
                        return Ok(GetObjectResult::NotModified);
                    }
                    _ => {
                        return Err(ObjectStoreError::Internal(err.to_string()));
                    }
                };
            }
        }
    }

    fn list_objects(
        &self,
        cmd: ListObjectsRequest,
    ) -> impl Stream<Item = Result<ObjectMetadata, ObjectStoreError>> {
        let client = self.client.clone();
        let mut paginator = client
            .list_objects_v2()
            .bucket(cmd.bucket)
            .set_prefix(cmd.prefix)
            .into_paginator()
            .send();

        Box::pin(try_stream!(while let Some(page) = paginator.next().await {
            let page = page.map_err(|err| ObjectStoreError::Internal(err.to_string()))?;

            for object in page.contents() {
                let key = object.key().ok_or_else(|| {
                    ObjectStoreError::Internal("S3 listed an object without a key".into())
                })?;

                let etag = object.e_tag().ok_or_else(|| {
                    ObjectStoreError::Internal("S3 did not return etag for object".into())
                })?;

                let size = object
                    .size()
                    .and_then(|size| u64::try_from(size).ok())
                    .ok_or_else(|| {
                        ObjectStoreError::Internal("S3 returned an invalid object size".into())
                    })?;

                yield ObjectMetadata {
                    key: key.to_owned(),
                    etag: etag.to_owned(),
                    size,
                };
            }
        }))
    }
}

#[async_trait]
impl ObjectStoreWriter for S3Store {
    async fn put_object(
        &self,
        cmd: PubObjectRequest,
    ) -> Result<PutObjectResponse, ObjectStoreError> {
        let client = self.client.clone();
        let mut request = client
            .put_object()
            .bucket(cmd.bucket)
            .body(into_aws_byte_stream(cmd.body))
            .key(cmd.key);

        if let Some(condition) = cmd.condition {
            match condition {
                PutObjectCondition::IfNoneMatch => {
                    request = request.if_none_match("*");
                }
                PutObjectCondition::IfMatch(etag) => {
                    request = request.if_match(etag);
                }
            }
        }

        let response = request.send().await.map_err(|err| {
            let code = err.code();
            let status = err
                .raw_response()
                .map(|response| response.status().as_u16());
            match (code, status) {
                (Some("PreConditionFailed"), _) | (_, Some(412)) => {
                    ObjectStoreError::PreConditionFailed(err.to_string())
                }
                (Some("ConditionalRequestFailed"), _) | (_, Some(409)) => {
                    ObjectStoreError::Conflict(err.to_string())
                }
                _ => ObjectStoreError::Internal(err.to_string()),
            }
        })?;

        let etag = response.e_tag.ok_or_else(|| {
            ObjectStoreError::Internal("S3 did not return etag for object".into())
        })?;

        Ok(PutObjectResponse { etag })
    }
}

#[async_trait]
impl ObjectStoreOwner for S3Store {
    async fn create_bucket(
        &self,
        cmd: CreateBucketInput,
    ) -> Result<CreateBucketOutput, ObjectStoreError> {
        let client = self.client.clone();
        let location_constraint = s3::types::BucketLocationConstraint::from(cmd.region.as_str());
        let create_bucket_config = s3::types::CreateBucketConfiguration::builder()
            .set_location_constraint(Some(location_constraint))
            .build();

        let response = client
            .create_bucket()
            .bucket(cmd.name)
            .create_bucket_configuration(create_bucket_config)
            .send()
            .await
            .map_err(|err| {
                if let Some(se) = err.as_service_error() {
                    if se.is_bucket_already_exists() {
                        return ObjectStoreError::ObjectAlreadyExists(
                            "bucket already exists".to_owned(),
                        );
                    }
                }
                return ObjectStoreError::Internal(err.to_string());
            })?;

        let location = response.location.ok_or_else(|| {
            return ObjectStoreError::Internal("bucket location is not set in response".to_owned());
        })?;

        let arn = response.bucket_arn.ok_or_else(|| {
            return ObjectStoreError::Internal("bucket arn is not set in response".to_owned());
        })?;

        Ok(CreateBucketOutput { location, arn })
    }
}
