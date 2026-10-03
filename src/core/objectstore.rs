use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use std::pin::Pin;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ObjectStoreError {
    #[error("internal error: {0}")]
    Internal(String),
    #[error("object already exists: {0}")]
    ObjectAlreadyExists(String),
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conditional write failed: {0}")]
    PreConditionFailed(String),
    #[error("conflicting operation: {0}")]
    Conflict(String),
}

pub struct GetObjectRequest {
    pub bucket: String,
    pub if_none_match: Option<String>,
    pub key: String,
}
pub type ReadObjectStream =
    Pin<Box<dyn Stream<Item = Result<Bytes, ObjectStoreError>> + Send + 'static>>;

pub struct GetObjectResponse {
    pub body: ReadObjectStream,
    pub etag: String,
    pub content_length: u64,
}

pub enum GetObjectResult {
    Found(GetObjectResponse),
    NotModified,
}

pub struct ListObjectsRequest {
    pub bucket: String,
    pub prefix: Option<String>,
}

pub struct ObjectMetadata {
    pub key: String,
    pub etag: String,
    pub size: u64,
}

#[async_trait]
pub trait ObjectStoreReader: Send + Sync {
    async fn get_object(&self, cmd: GetObjectRequest) -> Result<GetObjectResult, ObjectStoreError>;

    fn list_objects(
        &self,
        cmd: ListObjectsRequest,
    ) -> impl Stream<Item = Result<ObjectMetadata, ObjectStoreError>>;
}

pub enum PutObjectCondition {
    IfNoneMatch,
    IfMatch(String),
}

#[derive(Debug, Error)]
pub enum UploadBodyError {
    #[error("upload source ended unexpectedly")]
    UnexpectedEOF,
    #[error("internal error: {0}")]
    Internal(String),
}

pub type WriteObjectStream =
    Pin<Box<dyn Stream<Item = Result<Bytes, UploadBodyError>> + Send + Sync>>;

pub enum WriteObjectBody {
    Bytes(Bytes),
    Stream {
        stream: WriteObjectStream,
        content_length: u64,
    },
}

pub struct PubObjectRequest {
    pub bucket: String,
    pub key: String,
    pub body: WriteObjectBody,
    pub condition: Option<PutObjectCondition>,
}

pub struct PutObjectResponse {
    pub etag: String,
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

pub struct CreateBucketInput {
    pub name: String,
    pub region: String,
}

pub struct CreateBucketOutput {
    pub location: String,
    pub arn: String,
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
