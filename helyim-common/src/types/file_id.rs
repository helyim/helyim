use std::fmt::{Display, Formatter};

use crate::types::{VolumeId, needle::NeedleId};

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
