use std::{fs, path::Path, sync::Arc};

use faststr::FastStr;
use once_cell::sync::Lazy;
use regex::Regex;

use crate::storage::{
    disk_location::DiskLocation,
    erasure_coding::{volume::EcVolume, EcVolumeError, EcVolumeRef, EcVolumeShard, ShardId},
    VolumeError, VolumeId,
};

static REGEX: Lazy<Regex> = Lazy::new(|| Regex::new("\\.ec[0-9][0-9]").unwrap());

impl DiskLocation {
    pub fn find_ec_volume(&self, vid: VolumeId) -> Option<EcVolumeRef> {
        self.ec_volumes
            .get(&vid)
            .map(|volume| volume.value().clone())
    }

    pub async fn destroy_ec_volume(&self, vid: VolumeId) -> Result<(), EcVolumeError> {
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
            return ec_volume.find_shard(shard_id).await;
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
                self.ec_volumes.entry(vid).or_insert(Arc::new(volume))
            }
        };
        volume.add_shard(shard).await;
        Ok(())
    }

    pub async fn unload_ec_shard(&self, vid: VolumeId, shard_id: ShardId) -> bool {
        match self.ec_volumes.get(&vid) {
            Some(volume) => {
                if volume.delete_shard(shard_id).await.is_some() && volume.shards_len().await == 0 {
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
            let ext = Path::new(shard).extension().unwrap();
            let shard_id = ext.to_string_lossy()[3..].parse::<u64>()?;
            self.load_ec_shard(collection, vid, shard_id as ShardId)
                .await?;
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
                    match parse_volume_id(base_name) {
                        Ok((vid, collection)) => {
                            if let Some(m) = REGEX.find(&ext.to_string_lossy()) {
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
                        Err(err) => continue,
                    }
                }
            }
        }
        Ok(())
    }
}

fn parse_volume_id(name: &str) -> Result<(VolumeId, &str), VolumeError> {
    let index = name.find('_').unwrap_or_default();
    let (collection, vid) = (&name[0..index], &name[index + 1..]);
    Ok((vid.parse()?, collection))
}
