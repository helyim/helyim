use std::fmt::{Display, Formatter};

use super::VolumeId;
use crate::storage::NeedleId;

pub struct FileId {
    pub volume_id: VolumeId,
    pub key: NeedleId,
    pub hash: u32,
}

impl Display for FileId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{},{:x}{:08x}", self.volume_id, self.key, self.hash)
    }
}
