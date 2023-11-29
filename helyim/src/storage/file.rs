use std::{
    fmt::{Display, Formatter},
    fs::File,
    sync::Arc,
};

use tokio::sync::RwLock;

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

#[derive(Clone)]
pub struct FileRef(Arc<RwLock<File>>);

impl FileRef {
    pub fn new(file: File) -> Self {
        Self(Arc::new(RwLock::new(file)))
    }

    pub async fn read(&self) -> tokio::sync::RwLockReadGuard<'_, File> {
        self.0.read().await
    }

    pub async fn write(&self) -> tokio::sync::RwLockWriteGuard<'_, File> {
        self.0.write().await
    }
}
