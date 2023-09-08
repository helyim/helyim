use std::{collections::HashMap, fs, path::Path};

use faststr::FastStr;
use tracing::info;

use crate::{
    anyhow,
    errors::Result,
    storage::{
        needle_map::NeedleMapType,
        replica_placement::ReplicaPlacement,
        ttl::Ttl,
        volume::{Volume, DATA_FILE_SUFFIX},
        VolumeId,
    },
};

pub struct DiskLocation {
    pub directory: FastStr,
    pub max_volume_count: i64,
    pub volumes: HashMap<VolumeId, Volume>,

    pub(crate) shutdown_rx: async_broadcast::Receiver<()>,
}

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
            shutdown_rx,
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
                info!("load volume for dat file {:?}", fpath);
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
                    self.shutdown_rx.clone(),
                )?;
                info!("add volume: {}", vid);
                self.volumes.insert(vid, volume);
            }
        }

        Ok(())
    }

    pub fn delete_volume(&mut self, vid: VolumeId) -> Result<()> {
        if let Some(v) = self.volumes.get_mut(&vid) {
            v.destroy()?;
        }
        self.volumes.remove(&vid);
        Ok(())
    }
}
