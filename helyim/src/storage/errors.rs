use std::time::SystemTimeError;

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

use crate::storage::{version::Version, NeedleId, VolumeId};

#[derive(thiserror::Error, Debug)]
pub enum NeedleError {
    #[error("Io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("error: {0}")]
    BoxError(#[from] Box<dyn std::error::Error + Sync + Send>),
    #[error("Parse integer error: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error("Volume {0}: needle {1} has deleted.")]
    Deleted(VolumeId, u64),
    #[error("Replicate delete needle {1} failed, volume: {0}, location: {2}, error: {3}")]
    ReplicateDelete(VolumeId, NeedleId, String, String),
    #[error("Replicate write needle {1} failed, volume: {0}, location: {2}, error: {3}")]
    ReplicateWrite(VolumeId, NeedleId, String, String),
    #[error("Volume {0}: needle {1} has expired.")]
    Expired(VolumeId, u64),
    #[error("Needle {0} not found.")]
    NotFound(u64),
    #[error("Cookie not match, needle cookie is {0} but got {1}")]
    CookieNotMatch(u32, u32),
    #[error("Unsupported version: {0}")]
    UnsupportedVersion(Version),
    #[error("Crc error, read: {0}, calculate: {1}, may be data on disk corrupted")]
    Crc(u32, u32),
    #[error("Invalid file id: {0}")]
    InvalidFid(String),
    #[error("key hash: {0} is too short or too long")]
    InvalidKeyHash(String),
}

#[derive(thiserror::Error, Debug)]
pub enum VolumeError {
    #[error("Io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("error: {0}")]
    BoxError(#[from] Box<dyn std::error::Error + Sync + Send>),
    #[error("Errno: {0}")]
    Errno(#[from] rustix::io::Errno),
    #[error("System time error: {0}")]
    SystemTimeError(#[from] SystemTimeError),
    #[error("Raw volume error: {0}")]
    String(String),
    #[error("Parse integer error: {0}")]
    ParseInt(#[from] std::num::ParseIntError),

    #[error("Tonic status: {0}")]
    TonicStatus(#[from] tonic::Status),

    #[error("Invalid replica placement: {0}")]
    ReplicaPlacement(String),
    #[error("No writable volumes.")]
    NoWritableVolumes,
    #[error("Volume {0} is not found.")]
    NotFound(VolumeId),
    #[error("Data integrity error: {0}")]
    DataIntegrity(String),
    #[error("Volume {0} is not loaded.")]
    NotLoad(VolumeId),
    #[error("Volume {0} has loaded.")]
    HasLoaded(VolumeId),
    #[error("Volume {0} is readonly.")]
    Readonly(VolumeId),
    #[error("Needle error: {0}")]
    Needle(#[from] NeedleError),
    #[error("No free space: {0}")]
    NoFreeSpace(String),
}

impl From<nom::Err<nom::error::Error<&str>>> for VolumeError {
    fn from(value: nom::Err<nom::error::Error<&str>>) -> Self {
        Self::String(value.to_string())
    }
}

impl IntoResponse for VolumeError {
    fn into_response(self) -> Response {
        let error = self.to_string();
        let error = json!({
            "error": error
        });
        let response = (StatusCode::BAD_REQUEST, Json(error));
        response.into_response()
    }
}
