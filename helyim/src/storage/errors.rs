use std::time::SystemTimeError;

use crate::storage::{version::Version, VolumeId};

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
    #[error("Volume {0}: needle {1} has expired.")]
    Expired(VolumeId, u64),
    #[error("Volume {0}: needle {1} not found.")]
    NotFound(VolumeId, u64),
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

    #[error("Futures channel send error: {0}")]
    SendError(#[from] futures::channel::mpsc::SendError),
    #[error("Oneshot channel canceled")]
    OneshotCanceled(#[from] futures::channel::oneshot::Canceled),

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
}
