use std::fs::{read, write};

use helyim_proto::VolumeInfo;
use tracing::error;

use crate::{anyhow, storage::VolumeError, util::file::check_file};

pub fn maybe_load_volume_info(filename: &str) -> Result<Option<VolumeInfo>, VolumeError> {
    match check_file(filename)? {
        Some((false, _, _, _)) | None => Ok(None),
        Some(_) => {
            let tier_data = read(filename)?;
            let volume_info: VolumeInfo = serde_json::from_slice(&tier_data)?;
            if volume_info.files.is_empty() {
                return Ok(None);
            }
            Ok(Some(volume_info))
        }
    }
}

pub fn save_volume_info(filename: &str, volume_info: VolumeInfo) -> Result<(), VolumeError> {
    match check_file(filename)? {
        Some((_, false, _, _)) => {
            error!("{filename} not writable.");
            Err(anyhow!("{filename} not writable."))
        }
        Some(_) | None => {
            let data = serde_json::to_vec(&volume_info)?;
            Ok(write(filename, data)?)
        }
    }
}
