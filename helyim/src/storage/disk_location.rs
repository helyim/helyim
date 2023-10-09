use std::{fs, path::Path, sync::Arc};

use dashmap::{mapref::one::Ref, DashMap};
use faststr::FastStr;
use futures::channel::mpsc::unbounded;
use nom::{bytes::complete::take_till, character::complete::char, combinator::opt, sequence::pair};
use once_cell::sync::Lazy;
use regex::Regex;
use tracing::info;

use crate::{
    anyhow,
    errors::Result,
    rt_spawn,
    storage::{
        erasure_coding::{ec_volume_loop, EcVolume, EcVolumeEventTx, EcVolumeShard, ShardId},
        needle_map::NeedleMapType,
        replica_placement::ReplicaPlacement,
        ttl::Ttl,
        volume::{volume_loop, Volume, VolumeEventTx, DATA_FILE_SUFFIX},
        VolumeId,
    },
};

static REGEX: Lazy<Regex> = Lazy::new(|| Regex::new("\\.ec[0-9][0-9]").unwrap());

pub struct DiskLocation {
    pub directory: FastStr,
    pub max_volume_count: i64,
    pub volumes: DashMap<VolumeId, VolumeEventTx>,
    pub ec_volumes: DashMap<VolumeId, EcVolumeEventTx>,
    pub(crate) shutdown: async_broadcast::Receiver<()>,
}

unsafe impl Send for DiskLocation {}
unsafe impl Sync for DiskLocation {}

impl DiskLocation {
    pub fn new(
        dir: &str,
        max_volume_count: i64,
        shutdown_rx: async_broadcast::Receiver<()>,
    ) -> DiskLocation {
        DiskLocation {
            directory: FastStr::new(dir),
            max_volume_count,
            volumes: DashMap::new(),
            ec_volumes: DashMap::new(),
            shutdown: shutdown_rx,
        }
    }

    pub fn load_existing_volumes(&mut self, needle_map_type: NeedleMapType) -> Result<()> {
        // TODO concurrent load volumes
        let dir = self.directory.to_string();
        let dir = Path::new(&dir);
        info!("load existing volumes dir: {}", self.directory);
        for entry in fs::read_dir(dir)? {
            let file = entry?.path();
            let path = file.as_path();

            if path.extension().unwrap_or_default() == DATA_FILE_SUFFIX {
                let (vid, collection) = parse_volume_id_from_path(path)?;
                info!("load volume {}'s data file {:?}", vid, path);
                if let dashmap::mapref::entry::Entry::Vacant(entry) = self.volumes.entry(vid) {
                    let volume = Volume::new(
                        self.directory.clone(),
                        FastStr::new(collection),
                        vid,
                        needle_map_type,
                        ReplicaPlacement::default(),
                        Ttl::default(),
                        0,
                    )?;
                    let (tx, rx) = unbounded();
                    let volume_tx = VolumeEventTx::new(tx);
                    rt_spawn(volume_loop(volume, rx, self.shutdown.clone()));
                    entry.insert(volume_tx);
                }
            }
        }

        Ok(())
    }

    pub async fn delete_volume(&self, vid: VolumeId) -> Result<()> {
        if let Some((vid, v)) = self.volumes.remove(&vid) {
            v.destroy().await?;
            info!(
                "remove volume {vid} success, where disk location is {}",
                self.directory
            );
        }
        Ok(())
    }

    // erasure coding
    pub fn find_ec_volume(&self, vid: VolumeId) -> Option<Ref<VolumeId, EcVolumeEventTx>> {
        self.ec_volumes.get(&vid)
    }

    pub async fn destroy_ec_volume(&self, vid: VolumeId) -> Result<()> {
        if let Some((_, volume)) = self.ec_volumes.remove(&vid) {
            volume.destroy().await?;
        }
        Ok(())
    }

    pub async fn find_ec_shard(
        &self,
        vid: VolumeId,
        shard_id: ShardId,
    ) -> Result<Option<Arc<EcVolumeShard>>> {
        if let Some(ec_volume) = self.ec_volumes.get(&vid) {
            return ec_volume.find_shard(shard_id).await;
        }
        Ok(None)
    }

    pub async fn load_ec_shard(
        &self,
        collection: &str,
        vid: VolumeId,
        shard_id: ShardId,
    ) -> Result<()> {
        let collection = FastStr::new(collection);
        let shard = EcVolumeShard::new(self.directory.clone(), collection.clone(), vid, shard_id)?;
        let volume = match self.ec_volumes.get_mut(&vid) {
            Some(volume) => volume,
            None => {
                let (tx, rx) = unbounded();
                let volume = EcVolume::new(self.directory.clone(), collection, vid)?;
                rt_spawn(ec_volume_loop(volume, rx, self.shutdown.clone()));
                self.ec_volumes
                    .entry(vid)
                    .or_insert(EcVolumeEventTx::new(tx))
            }
        };
        volume.add_shard(shard).await?;
        Ok(())
    }

    pub async fn unload_ec_shard(&self, vid: VolumeId, shard_id: ShardId) -> Result<bool> {
        match self.ec_volumes.get(&vid) {
            Some(volume) => {
                if volume.delete_shard(shard_id).await?.is_some() && volume.shards_len().await? == 0
                {
                    self.ec_volumes.remove(&vid);
                }
                Ok(true)
            }
            None => Ok(false),
        }
    }

    pub async fn load_ec_shards(
        &mut self,
        shards: &[String],
        collection: &str,
        vid: VolumeId,
    ) -> Result<()> {
        for shard in shards {
            let shard_id = u64::from_str_radix(&shard[3..], 16)?;
            self.load_ec_shard(collection, vid, shard_id as ShardId)
                .await?;
        }
        Ok(())
    }

    pub async fn load_all_shards(&mut self) -> Result<()> {
        let dir = fs::read_dir(self.directory.to_string())?;
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

                            if ext.eq_ignore_ascii_case("ecx") && vid == pre_volume_id {
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

fn parse_volume_id(name: &str) -> Result<(VolumeId, &str)> {
    let index = name.find('_').unwrap_or_default();
    let (collection, vid) = (&name[0..index], &name[index + 1..]);
    let vid = u32::from_str_radix(vid, 16)?;
    Ok((vid, collection))
}

fn parse_volume_id_from_path(path: &Path) -> Result<(VolumeId, &str)> {
    if path.is_dir() {
        return Err(anyhow!(
            "invalid data file: {}",
            path.to_str().unwrap_or_default()
        ));
    }

    let name = path.file_name().unwrap().to_str().unwrap();
    let name = &name[..name.len() - 4];

    let (collection, id) =
        pair(take_till(|c| c == '_'), opt(char('_')))(name).map(|(input, (left, opt_char))| {
            match opt_char {
                Some(_) => (left, input),
                None => (input, left),
            }
        })?;
    let vid = id.parse()?;
    Ok((vid, collection))
}
