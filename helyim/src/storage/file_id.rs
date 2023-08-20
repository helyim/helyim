use std::fmt::{Display, Formatter};

use super::VolumeId;

pub struct FileId {
    pub volume_id: VolumeId,
    pub key: u64,
    pub hash_code: u32,
}

impl Display for FileId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{},{:x}{:08x}", self.volume_id, self.key, self.hash_code)
    }
}
