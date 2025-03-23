use std::{
    fs::{read, write},
    io,
    io::ErrorKind,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use faststr::FastStr;
use helyim_common::{file::check_file, types::VolumeId};
use helyim_proto::volume::VolumeInfo;
use serde::{Deserialize, Serialize};

use crate::{ShardId, TOTAL_SHARDS_COUNT};

#[derive(Clone, Serialize, Deserialize)]
pub struct ShardBits(Arc<AtomicU32>);

impl ShardBits {
    fn new(value: u32) -> Self {
        Self(Arc::new(AtomicU32::new(value)))
    }

    fn value(&self) -> u32 {
        self.0.load(Ordering::Relaxed)
    }

    pub fn add_shard_id(self, id: ShardId) -> Self {
        Self::new(self.value() | (1 << id))
    }

    pub fn remove_shard_id(self, id: ShardId) -> Self {
        Self::new(self.value() & !(1 << id))
    }

    pub fn has_shard_id(&self, id: ShardId) -> bool {
        self.value() & (1 << id) > 0
    }

    pub fn shard_ids(&self) -> Vec<ShardId> {
        let mut shards = Vec::new();
        for shard in 0..TOTAL_SHARDS_COUNT {
            let id = shard as ShardId;
            if self.has_shard_id(id) {
                shards.push(id);
            }
        }
        shards
    }

    pub fn shard_id_count(&self) -> u64 {
        let mut count = 0;
        loop {
            let mut v = self.value();
            if v == 0 {
                return count;
            }
            v &= v - 1;
            self.0.store(v, Ordering::Relaxed);

            count += 1;
        }
    }

    pub fn minus(&self, other: &Self) -> Self {
        Self::new(self.value() & !other.value())
    }

    pub fn plus(&self, other: &Self) -> Self {
        Self::new(self.value() | other.value())
    }
}

impl From<u32> for ShardBits {
    fn from(value: u32) -> Self {
        Self::new(value)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct EcVolumeInfo {
    pub volume_id: VolumeId,
    pub collection: FastStr,
    pub shard_bits: ShardBits,
}

impl EcVolumeInfo {
    pub fn new(collection: FastStr, volume_id: VolumeId, shard_bits: ShardBits) -> Self {
        Self {
            volume_id,
            collection,
            shard_bits,
        }
    }

    pub fn minus(&self, other: &EcVolumeInfo) -> Self {
        Self {
            volume_id: self.volume_id,
            collection: self.collection.clone(),
            shard_bits: self.shard_bits.minus(&other.shard_bits),
        }
    }
}

pub fn maybe_load_volume_info(filename: &str) -> Result<Option<VolumeInfo>, io::Error> {
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

pub fn save_volume_info(filename: &str, volume_info: VolumeInfo) -> Result<(), io::Error> {
    match check_file(filename)? {
        Some((_, false, _, _)) => Err(io::Error::new(
            ErrorKind::ReadOnlyFilesystem,
            format!("{filename} not writable."),
        )),
        Some(_) | None => {
            let data = serde_json::to_vec(&volume_info)?;
            Ok(write(filename, data)?)
        }
    }
}
