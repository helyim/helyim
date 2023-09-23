use std::{collections::HashMap, fs, fs::File, os::unix::fs::OpenOptionsExt, time::SystemTime};

use faststr::FastStr;

use crate::{
    errors::Result,
    storage::{
        version::{Version, VERSION3},
        VolumeId,
    },
};

mod decoder;
mod encoder;

pub type ShardId = u8;

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
    pub fn new(
        dir: FastStr,
        collection: FastStr,
        vid: VolumeId,
    ) -> Result<EcVolume> {
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
            version: VERSION3,
        })
    }
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
