use std::{collections::HashMap, fs, path::Path};

use faststr::FastStr;
use futures::{channel::mpsc::unbounded, future::join_all};
use helyim_macros::event_fn;
use nom::{bytes::complete::take_till, character::complete::char, combinator::opt, sequence::pair};
use tokio::task::JoinHandle;
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

#[event_fn]
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

    /// concurrent loading volumes
    pub async fn load_existing_volumes(&mut self, needle_map_type: NeedleMapType) -> Result<()> {
        let dir = self.directory.to_string();
        let dir = Path::new(&dir);
        info!("load existing volumes dir: {}", self.directory);

        let mut handles: Vec<JoinHandle<Result<(VolumeId, VolumeEventTx)>>> = vec![];
        for entry in fs::read_dir(dir)? {
            let file = entry?.path();
            let path = file.as_path();

            if path.extension().unwrap_or_default() == DATA_FILE_SUFFIX {
                let (vid, collection) = parse_volume_id_from_path(path)?;
                info!("load volume {} data file {:?}", vid, path);
                if !self.volumes.contains_key(&vid) {
                    let dir = self.directory.clone();
                    let shutdown = self.shutdown.clone();
                    let collection = FastStr::new(collection);

                    let handle = rt_spawn(async move {
                        let volume = Volume::new(
                            dir,
                            collection,
                            vid,
                            needle_map_type,
                            ReplicaPlacement::default(),
                            Ttl::default(),
                            0,
                        )?;
                        let (tx, rx) = unbounded();
                        let volume_tx = VolumeEventTx::new(tx);
                        rt_spawn(volume_loop(volume, rx, shutdown));
                        Ok((vid, volume_tx))
                    });
                    handles.push(handle);
                }
            }
        }

        for join in join_all(handles).await {
            let (vid, volume_tx) = join??;
            self.volumes.insert(vid, volume_tx);
        }

        Ok(())
    }

    pub fn add_volume(&mut self, vid: VolumeId, volume: VolumeEventTx) {
        self.volumes.insert(vid, volume);
    }
    pub fn shutdown_rx(&self) -> async_broadcast::Receiver<()> {
        self.shutdown.clone()
    }

    pub fn directory(&self) -> FastStr {
        self.directory.clone()
    }

    pub fn max_volume_count(&self) -> i64 {
        self.max_volume_count
    }
    pub fn get_volume(&self, vid: VolumeId) -> Option<VolumeEventTx> {
        self.volumes.get(&vid).cloned()
    }

    pub fn get_volumes(&self) -> HashMap<VolumeId, VolumeEventTx> {
        self.volumes.clone()
    }

    pub fn get_volumes_len(&self) -> usize {
        self.volumes.len()
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
    Ok((id.parse()?, collection))
}
