use std::{
    collections::{hash_map, HashMap},
    fs,
    path::Path,
};

use faststr::FastStr;
use futures::channel::mpsc::unbounded;
use nom::{bytes::complete::take_till, character::complete::char, combinator::opt, sequence::pair};
use tracing::info;

use crate::{
    anyhow,
    errors::Result,
    rt_spawn,
    storage::{
        erasure_coding::EcVolumeEventTx,
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
    pub ec_volumes: HashMap<VolumeId, EcVolumeEventTx>,
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
            ec_volumes: HashMap::new(),
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
                let (vid, collection) = parse_volume_id(path)?;
                info!("load volume {}'s data file {:?}", vid, path);
                if let hash_map::Entry::Vacant(entry) = self.volumes.entry(vid) {
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

    // erasure coding
    pub fn find_ec_volume(&self, vid: VolumeId) -> Option<&EcVolumeEventTx> {
        self.ec_volumes.get(&vid)
    }

    pub fn destroy_ec_volume(&mut self, vid: VolumeId) -> Result<()> {
        if let Some(volume) = self.ec_volumes.remove(&vid) {
            volume.destroy()?;
        }
        Ok(())
    }
}

fn parse_volume_id(path: &Path) -> Result<(VolumeId, &str)> {
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
