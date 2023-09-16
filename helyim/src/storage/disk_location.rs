use std::{collections::HashMap, fs, path::Path};

use faststr::FastStr;
use futures::channel::mpsc::unbounded;
use tracing::info;

use crate::{
    anyhow,
    errors::Result,
    rt_spawn,
    storage::{
        needle_map::NeedleMapType,
        replica_placement::ReplicaPlacement,
        ttl::Ttl,
        volume::{volume_loop, Volume, VolumeEventTx, DATA_FILE_SUFFIX},
        VolumeId,
    },
};

pub struct DiskLocation {
    pub directory: FastStr,
    pub max_volume_count: i64,
    pub volumes: HashMap<VolumeId, VolumeEventTx>,
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
            volumes: HashMap::new(),
            shutdown: shutdown_rx,
        }
    }

    pub fn parse_volume_id(&self, p: &Path) -> Result<(VolumeId, FastStr)> {
        if p.is_dir() || p.extension().unwrap_or_default() != DATA_FILE_SUFFIX {
            return Err(anyhow!("not valid file: {}", p.to_str().unwrap()));
        }

        let name = p.file_name().unwrap().to_str().unwrap();

        let (collection, id) = match name.find('_') {
            Some(idx) => (&name[0..idx], &name[idx + 1..name.len() - 4]),
            None => (&name[0..0], &name[0..name.len() - 4]),
        };

        let vid = id.parse()?;

        Ok((vid, FastStr::new(collection)))
    }

    pub fn load_existing_volumes(&mut self, needle_map_type: NeedleMapType) -> Result<()> {
        // TODO concurrent load volumes
        let dir = self.directory.to_string();
        let dir = Path::new(&dir);
        info!("load existing volumes dir: {}", self.directory);
        for entry in fs::read_dir(dir)? {
            let file = entry?.path();
            let fpath = file.as_path();

            if fpath.extension().unwrap_or_default() == DATA_FILE_SUFFIX {
                info!("load volume for data file {:?}", fpath);
                let (vid, collection) = self.parse_volume_id(fpath)?;
                if self.volumes.get(&vid).is_some() {
                    continue;
                }
                let volume = Volume::new(
                    self.directory.clone(),
                    collection,
                    vid,
                    needle_map_type,
                    ReplicaPlacement::default(),
                    Ttl::default(),
                    0,
                )?;
                let (tx, rx) = unbounded();
                let volume_tx = VolumeEventTx::new(tx);
                rt_spawn(volume_loop(volume, rx, self.shutdown.clone()));
                self.volumes.insert(vid, volume_tx);
            }
        }

        Ok(())
    }

    pub async fn delete_volume(&mut self, vid: VolumeId) -> Result<()> {
        if let Some(v) = self.volumes.remove(&vid) {
            v.destroy().await?;
            info!(
                "remove volume {vid} success, where disk location is {}",
                self.directory
            );
        }
        Ok(())
    }
}
