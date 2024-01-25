use std::{fs, fs::File, os::unix::fs::OpenOptionsExt};

use faststr::FastStr;

use crate::storage::{
    erasure_coding::{errors::EcShardError, to_ext, ShardId},
    VolumeId,
};

pub struct EcVolumeShard {
    pub shard_id: ShardId,
    pub volume_id: VolumeId,
    pub collection: FastStr,
    dir: FastStr,
    pub ecd_file: File,
    pub ecd_filesize: u64,
}

impl EcVolumeShard {
    pub fn new(
        dir: FastStr,
        collection: FastStr,
        volume_id: VolumeId,
        id: ShardId,
    ) -> Result<Self, std::io::Error> {
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

    pub fn destroy(&self) -> Result<(), EcShardError> {
        fs::remove_file(format!("{}{}", self.filename(), to_ext(self.shard_id)))?;
        Ok(())
    }
}

pub fn ec_shard_filename(collection: &str, dir: &str, volume_id: VolumeId) -> String {
    if collection.is_empty() {
        format!("{}/{}", dir, volume_id)
    } else {
        format!("{}/{}_{}", dir, collection, volume_id)
    }
}

pub fn ec_shard_base_filename(collection: &str, volume_id: VolumeId) -> String {
    if collection.is_empty() {
        format!("{}", volume_id)
    } else {
        format!("{}_{}", collection, volume_id)
    }
}
