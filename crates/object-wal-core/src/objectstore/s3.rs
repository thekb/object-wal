use super::ports::*;
use async_trait::async_trait;
use aws_sdk_s3 as s3;
use aws_sdk_s3::primitives::{ByteStream, SdkBody};
use futures::TryStreamExt;
use http_body::Frame;
use http_body_util::StreamBody;

fn into_aws_byte_stream(stream: ObjectStream) -> ByteStream {
    let frames = stream.map_ok(Frame::data);
    let body = StreamBody::new(frames);
    let sdk_body = SdkBody::from_body_1_x(body);

    ByteStream::new(sdk_body)
}

pub struct S3ObjectStore {
    client: s3::Client,
}

#[async_trait]
impl ObjectStoreOwner for S3ObjectStore {
    async fn create_bucket(
        &self,
        cmd: CreateBucketInput,
    ) -> Result<CreateBucketOutput, ObjectStoreError> {
        let constraint = s3::types::BucketLocationConstraint::from(cmd.region.as_str());
        let cfg = s3::types::CreateBucketConfiguration::builder()
            .location_constraint(constraint)
            .build();

        let response = self
            .client
            .create_bucket()
            .create_bucket_configuration(cfg)
            .bucket(cmd.name)
            .send()
            .await
            .map_err(|err| {
                if let Some(se) = err.as_service_error() {
                    if se.is_bucket_already_exists() || se.is_bucket_already_owned_by_you() {
                        return ObjectStoreError::ObjectAlreadyExists(err.to_string());
                    }
                }
                return ObjectStoreError::InternalError(err.to_string());
            })?;

        Ok(CreateBucketOutput {
            location: response.location,
            arn: response.bucket_arn,
        })
    }
}

#[async_trait]
impl ObjectStoreReader for S3ObjectStore {
    async fn get_object(
        &self,
        cmd: GetObjectRequest,
    ) -> Result<GetObjectResponse, ObjectStoreError> {
        let mut request = self.client.get_object().bucket(cmd.bucket).key(cmd.key);
        if let Some(etag) = cmd.if_none_match {
            request = request.if_none_match(etag);
        }
        let response = request.send().await.map_err(|err| {
            if let Some(se) = err.as_service_error() {
                if se.is_no_such_key() {
                    return ObjectStoreError::NotFound(err.to_string());
                }
            }
            return ObjectStoreError::InternalError(err.to_string());
        })?;

        let body = futures::stream::unfold(response.body, |mut byte_stream| async move {
            byte_stream.next().await.map(|result| {
                let result = result.map_err(|err| {
                    ObjectStoreError::InternalError(format!(
                        "failed to read S3 object body: {0}",
                        err.to_string()
                    ))
                });
                (result, byte_stream)
            })
        });

        Ok(GetObjectResponse {
            body: Box::pin(body),
            etag: response.e_tag,
            content_length: response.content_length,
        })
    }

    async fn list_objects(
        &self,
        cmd: ListObjectsRequest,
    ) -> Result<ListObjectsResponse, ObjectStoreError> {
        let mut request = self.client.list_objects_v2().bucket(cmd.bucket);
        if let Some(max_keys) = cmd.max_keys {
            request = request.max_keys(max_keys);
        } else {
            request = request.max_keys(10);
        }
        let mut response = request.into_paginator().send();

        let mut objects = Vec::new();

        while let Some(result) = response.next().await {
            let page = result.map_err(|err| {
                if let Some(se) = err.as_service_error() {
                    if se.is_no_such_bucket() {
                        return ObjectStoreError::NotFound(format!(
                            "bucket does not exist: {0}",
                            se.to_string()
                        ));
                    }
                }
                return ObjectStoreError::InternalError(err.to_string());
            })?;

            if let Some(contents) = page.contents {
                for object in contents {
                    objects.push(ObjectMetadata {
                        key: object.key,
                        etag: object.e_tag,
                        size: object.size,
                    });
                }
            }
        }

        Ok(ListObjectsResponse { items: objects })
    }
}

#[async_trait]
impl ObjectStoreWriter for S3ObjectStore {
    async fn put_object(
        &self,
        cmd: PubObjectRequest,
    ) -> Result<PutObjectResponse, ObjectStoreError> {
        let mut request = self
            .client
            .put_object()
            .bucket(cmd.bucket)
            .body(into_aws_byte_stream(cmd.body))
            .key(cmd.key);
        if let Some(if_none_match) = cmd.if_none_match {
            request = request.if_none_match(if_none_match);
        }

        let response = request.send().await.map_err(|err| {
            ObjectStoreError::BadRequest(format!("failed to put object: {}", err.to_string()))
        })?;

        Ok(PutObjectResponse {
            etag: response.e_tag,
        })
    }
}
