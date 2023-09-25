use std::{collections::HashMap, fs, fs::File, os::unix::fs::OpenOptionsExt, time::SystemTime};

use faststr::FastStr;
use helyim_proto::VolumeInfo;

use crate::{
    errors::Result,
    proto::{maybe_load_volume_info, save_volume_info},
    storage::{
        version::{Version, VERSION3},
        NeedleId, VolumeId,
    },
};

mod decoder;
mod encoder;
mod locate;
mod volume_info;

pub type ShardId = u8;

pub const DATA_SHARDS_COUNT: u32 = 10;
pub const PARITY_SHARDS_COUNT: u32 = 4;
pub const TOTAL_SHARDS_COUNT: u32 = DATA_SHARDS_COUNT + PARITY_SHARDS_COUNT;
pub const ERASURE_CODING_LARGE_BLOCK_SIZE: u64 = 1024 * 1024 * 1024;
pub const ERASURE_CODING_SMALL_BLOCK_SIZE: u64 = 1024 * 1024;

pub struct EcVolume {
    volume_id: VolumeId,
    dir: FastStr,
    collection: FastStr,
    ecx_file: File,
    ecx_filesize: u64,
    ecx_created_at: SystemTime,
    shards: Vec<EcVolumeShard>,
    shard_locations: HashMap<ShardId, Vec<FastStr>>,
    shard_locations_refresh_time: SystemTime,
    version: Version,
    ecj_file: File,
}

impl EcVolume {
    pub fn new(dir: FastStr, collection: FastStr, vid: VolumeId) -> Result<EcVolume> {
        let base_filename = ec_shard_filename(&collection, &dir, vid);
        let ecx_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .mode(0o644)
            .open(format!("{}.ecx", base_filename))?;
        let ecx_filesize = ecx_file.metadata()?.len();
        let ecx_created_at = ecx_file.metadata()?.created()?;

        let ecj_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o644)
            .open(format!("{}.ecj", base_filename))?;

        let mut version = VERSION3;
        let filename = format!("{}.vif", base_filename);
        if let Some(volume_info) = maybe_load_volume_info(&filename)? {
            version = volume_info.version as Version;
        } else {
            let volume_info = VolumeInfo {
                version: version as u32,
                ..Default::default()
            };
            save_volume_info(&filename, volume_info)?;
        }

        Ok(EcVolume {
            volume_id: vid,
            dir,
            collection,
            ecx_file,
            ecx_filesize,
            ecx_created_at,
            ecj_file,
            shards: Vec::new(),
            shard_locations: HashMap::new(),
            shard_locations_refresh_time: SystemTime::now(),
            version,
        })
    }

    pub fn add_shard(&mut self, shard: EcVolumeShard) -> bool {
        for item in self.shards.iter() {
            if shard.shard_id == item.shard_id {
                return false;
            }
        }

        self.shards.push(shard);
        self.shards.sort_by(|left, right| {
            left.volume_id
                .cmp(&right.volume_id)
                .then(left.shard_id.cmp(&right.shard_id))
        });
        true
    }

    pub fn delete_shard(&mut self, shard_id: ShardId) -> Option<EcVolumeShard> {
        let mut idx = None;
        for (i, shard) in self.shards.iter().enumerate() {
            if shard.shard_id == shard_id {
                idx = Some(i);
            }
        }
        idx.map(|idx| self.shards.remove(idx))
    }

    pub fn find_shard(&self, shard_id: ShardId) -> Option<&EcVolumeShard> {
        self.shards.iter().find(|shard| shard.shard_id == shard_id)
    }

    pub fn filename(&self) -> String {
        ec_shard_filename(&self.collection, &self.dir, self.volume_id)
    }

    pub fn destroy(self) -> Result<()> {
        let filename = self.filename();
        for shard in self.shards {
            shard.destroy()?;
        }
        fs::remove_file(format!("{}.ecx", filename))?;
        fs::remove_file(format!("{}.ecj", filename))?;
        fs::remove_file(format!("{}.vif", filename))?;
        Ok(())
    }
}

fn search_needle_from_sorted_index<F>(
    ecx_file: File,
    ecx_filesize: u64,
    needle_id: NeedleId,
    process_needle: Option<F>,
) -> Result<(u32, u32)>
where
    F: FnMut(&mut File, u64) -> Result<()>,
{
    todo!()
}

pub struct EcVolumeShard {
    shard_id: ShardId,
    volume_id: VolumeId,
    collection: FastStr,
    dir: FastStr,
    ecd_file: File,
    ecd_filesize: u64,
}

impl EcVolumeShard {
    pub fn new(
        dir: FastStr,
        collection: FastStr,
        volume_id: VolumeId,
        id: ShardId,
    ) -> Result<Self> {
        let base_filename = ec_shard_filename(&collection, &dir, volume_id);
        let ecd_filename = format!("{}{}", base_filename, to_ext(id));
        let ecd_file = fs::OpenOptions::new()
            .read(true)
            .mode(0o644)
            .open(ecd_filename)?;
        let ecd_filesize = ecd_file.metadata()?.len();
        Ok(EcVolumeShard {
            shard_id: id,
            volume_id,
            collection,
            dir,
            ecd_file,
            ecd_filesize,
        })
    }

    pub fn filename(&self) -> String {
        ec_shard_filename(&self.collection, &self.dir, self.volume_id)
    }

    pub fn destroy(self) -> Result<()> {
        fs::remove_file(format!("{}{}", self.filename(), to_ext(self.shard_id)))?;
        Ok(())
    }
}

fn ec_shard_filename(collection: &str, dir: &str, volume_id: VolumeId) -> String {
    if collection.is_empty() {
        format!("{}{}", dir, volume_id)
    } else {
        format!("{}{}_{}", dir, collection, volume_id)
    }
}

fn ec_shard_base_filename(collection: &str, volume_id: VolumeId) -> String {
    if collection.is_empty() {
        format!("{}", volume_id)
    } else {
        format!("{}_{}", collection, volume_id)
    }
}

fn to_ext(ec_idx: ShardId) -> String {
    format!(".ec{:02}", ec_idx)
}
