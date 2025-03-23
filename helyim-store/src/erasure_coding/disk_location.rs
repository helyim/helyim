use std::{fs, io, path::Path, sync::Arc};

use dashmap::mapref::one::Ref;
use faststr::FastStr;
use helyim_common::{
    parser::{parse_collection_volume_id, parse_int},
    types::VolumeId,
};
use helyim_ec::{EcVolume, EcVolumeError, EcVolumeShard, ShardId};
use once_cell::sync::Lazy;
use regex::Regex;

use crate::disk_location::DiskLocation;

static REGEX: Lazy<Regex> = Lazy::new(|| Regex::new("ec[0-9][0-9]").unwrap());

impl DiskLocation {
    pub fn find_ec_volume(&self, vid: VolumeId) -> Option<Ref<VolumeId, EcVolume>> {
        self.ec_volumes.get(&vid)
    }

    pub async fn destroy_ec_volume(&self, vid: VolumeId) -> Result<(), io::Error> {
        if let Some((_, volume)) = self.ec_volumes.remove(&vid) {
            volume.destroy().await?;
        }
        Ok(())
    }

    pub async fn find_ec_shard(
        &self,
        vid: VolumeId,
        shard_id: ShardId,
    ) -> Option<Arc<EcVolumeShard>> {
        if let Some(ec_volume) = self.ec_volumes.get(&vid) {
            return ec_volume.find_ec_shard(shard_id).await;
        }
        None
    }

    pub async fn load_ec_shard(
        &self,
        collection: &str,
        vid: VolumeId,
        shard_id: ShardId,
    ) -> Result<(), EcVolumeError> {
        let collection = FastStr::new(collection);
        let shard = EcVolumeShard::new(self.directory.clone(), collection, vid, shard_id)?;
        let volume = match self.ec_volumes.get_mut(&vid) {
            Some(volume) => volume,
            None => {
                let volume = EcVolume::new(self.directory.clone(), shard.collection.clone(), vid)?;
                self.ec_volumes.entry(vid).or_insert(volume)
            }
        };
        volume.add_ec_shard(shard).await;
        Ok(())
    }

    pub async fn unload_ec_shard(&self, vid: VolumeId, shard_id: ShardId) -> bool {
        match self.ec_volumes.get(&vid) {
            Some(volume) => {
                if volume.delete_ec_shard(shard_id).await.is_some()
                    && volume.ec_shard_len().await == 0
                {
                    self.ec_volumes.remove(&vid);
                }
                true
            }
            None => false,
        }
    }

    pub async fn load_ec_shards(
        &self,
        shards: &[String],
        collection: &str,
        vid: VolumeId,
    ) -> Result<(), EcVolumeError> {
        for shard in shards {
            if let Some(index) = shard.find('.') {
                let ext = &shard[index + 1..];
                let shard_id = parse_int::<ShardId>(&ext[2..])?;
                self.load_ec_shard(collection, vid, shard_id as ShardId)
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn load_all_shards(&self) -> Result<(), EcVolumeError> {
        let dir = fs::read_dir(self.directory.as_str())?;
        let mut entries = Vec::new();
        for entry in dir {
            entries.push(entry?);
        }
        entries.sort_by_key(|entry| entry.file_name());

        let mut same_volume_shards = Vec::new();
        let mut pre_volume_id = 0 as VolumeId;

        for entry in entries.iter() {
            if entry.path().is_dir() {
                continue;
            }
            if let Some(filename) = entry.path().file_name() {
                let file_path = Path::new(filename);
                if let Some(ext) = file_path.extension() {
                    let filename = filename.to_string_lossy().to_string();
                    let base_name = &filename[..filename.len() - ext.len() - 1];

                    match parse_collection_volume_id(base_name) {
                        Ok((vid, collection)) => {
                            if REGEX.find(&ext.to_string_lossy()).is_some() {
                                if pre_volume_id == 0 || vid == pre_volume_id {
                                    same_volume_shards.push(filename);
                                } else {
                                    same_volume_shards = vec![filename];
                                }
                                pre_volume_id = vid;
                                continue;
                            }

                            if ext == "ecx" && vid == pre_volume_id {
                                self.load_ec_shards(&same_volume_shards, collection, vid)
                                    .await?;
                                pre_volume_id = vid;
                                continue;
                            }
                        }
                        Err(_err) => continue,
                    }
                }
            }
        }
        Ok(())
    }
}
