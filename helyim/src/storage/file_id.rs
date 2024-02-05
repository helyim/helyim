use std::fmt::{Display, Formatter};

use helyim_proto::filer::FileId as Pb_FileId;

use super::VolumeId;
use crate::storage::NeedleId;

#[derive(Copy, Clone)]
pub struct FileId {
    pub volume_id: VolumeId,
    pub key: NeedleId,
    pub hash: u32,
}

impl FileId {
    pub fn new(volume_id: VolumeId, key: NeedleId, hash: u32) -> Self {
        FileId {
            volume_id,
            key,
            hash,
        }
    }
}

impl Display for FileId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{},{:x}{:08x}", self.volume_id, self.key, self.hash)
    }
}

impl From<&Pb_FileId> for FileId {
    fn from(value: &Pb_FileId) -> Self {
        Self {
            volume_id: value.volume_id,
            key: value.file_key,
            hash: value.cookie,
        }
    }
}
