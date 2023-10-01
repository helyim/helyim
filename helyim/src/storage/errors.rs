use crate::storage::VolumeId;

#[derive(thiserror::Error, Debug)]
pub enum NeedleError {
    #[error("volume {0}: needle {1} has deleted.")]
    Deleted(VolumeId, u64),
    #[error("volume {0}: needle {1} has expired.")]
    Expired(VolumeId, u64),
    #[error("volume {0}: needle {1} not found.")]
    NotFound(VolumeId, u64),
    #[error("Cookie not match, needle cookie is {0} but got {1}")]
    CookieNotMatch(u32, u32),
}

#[derive(thiserror::Error, Debug)]
pub enum VolumeError {
    #[error("No writable volumes.")]
    NoWritableVolumes,
    #[error("Volume {0} is not found.")]
    NotFound(VolumeId),
    #[error("Data integrity error: {0}")]
    DataIntegrity(String),
}
