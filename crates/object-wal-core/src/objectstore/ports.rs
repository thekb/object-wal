use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::Stream;
use std::boxed::Box;
use std::pin::Pin;
use thiserror::Error;

pub struct CreateBucketInput {
    pub name: String,
    pub region: String,
}

pub struct CreateBucketOutput {
    pub location: Option<String>,
    pub arn: Option<String>,
}

#[derive(Debug, Error)]
pub enum ObjectStoreError {
    #[error("internal error: {0}")]
    InternalError(String),
    #[error("object already exists: {0}")]
    ObjectAlreadyExists(String),
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("not modified")]
    NotModified,
}

#[async_trait]
pub trait ObjectStoreOwner
where
    Self: Send + Sync,
{
    async fn create_bucket(
        &self,
        cmd: CreateBucketInput,
    ) -> Result<CreateBucketOutput, ObjectStoreError>;
}

pub struct GetObjectRequest {
    pub bucket: String,
    pub if_none_match: Option<String>,
    pub key: String,
}
/// ObjectStream is thread safe (Send), heap allocated (Box) with stable memory
/// address implementation of the Stream trait. This is needed to transparently
/// switch various object store providers without changing the WAL implementation.
pub type ObjectStream = Pin<Box<dyn Stream<Item = Result<Bytes, ObjectStoreError>> + Send + Sync>>;

pub struct GetObjectResponse {
    pub body: ObjectStream,
    pub etag: Option<String>,
    pub content_length: Option<i64>,
    // pub last_modified: DateTime<Utc>,
}

pub struct ListObjectsRequest {
    pub bucket: String,
    pub max_keys: Option<i32>,
    pub prefix: Option<String>,
}

pub struct ObjectMetadata {
    pub key: Option<String>,
    // pub last_modified: DateTime<Utc>,
    pub etag: Option<String>,
    pub size: Option<i64>,
}

pub struct ListObjectsResponse {
    pub items: Vec<ObjectMetadata>,
}

#[async_trait]
pub trait ObjectStoreReader
where
    Self: Send + Sync,
{
    async fn get_object(
        &self,
        cmd: GetObjectRequest,
    ) -> Result<GetObjectResponse, ObjectStoreError>;

    async fn list_objects(
        &self,
        cmd: ListObjectsRequest,
    ) -> Result<ListObjectsResponse, ObjectStoreError>;
}

pub struct PubObjectRequest {
    pub bucket: String,
    pub key: String,
    pub body: ObjectStream,
    pub if_none_match: Option<String>,
}

pub struct PutObjectResponse {
    pub etag: Option<String>,
}

#[async_trait]
pub trait ObjectStoreWriter
where
    Self: Send + Sync,
{
    async fn put_object(
        &self,
        cmd: PubObjectRequest,
    ) -> Result<PutObjectResponse, ObjectStoreError>;
}
