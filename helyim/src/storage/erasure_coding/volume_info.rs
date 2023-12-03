use faststr::FastStr;
use serde::{Deserialize, Serialize};

use crate::storage::{
    erasure_coding::{ShardId, TOTAL_SHARDS_COUNT},
    VolumeId,
};

#[derive(Copy, Clone, Serialize, Deserialize)]
pub struct ShardBits(u32);

impl ShardBits {
    pub fn add_shard_id(self, id: ShardId) -> Self {
        Self(self.0 | (1 << id))
    }

    pub fn remove_shard_id(self, id: ShardId) -> Self {
        Self(self.0 & !(1 << id))
    }

    pub fn has_shard_id(&self, id: ShardId) -> bool {
        self.0 & (1 << id) > 0
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

    pub fn shard_id_count(&mut self) -> u64 {
        let mut count = 0;
        loop {
            if self.0 == 0 {
                return count;
            }
            self.0 &= self.0 - 1;
            count += 1;
        }
    }

    pub fn minus(self, other: Self) -> Self {
        Self(self.0 & !other.0)
    }

    pub fn plus(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

impl From<u32> for ShardBits {
    fn from(value: u32) -> Self {
        Self(value)
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
            shard_bits: self.shard_bits.minus(other.shard_bits),
        }
    }
}
