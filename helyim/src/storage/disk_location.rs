use std::{collections::HashMap, fs, path::Path, result::Result as StdResult, sync::Arc};

use faststr::FastStr;
use futures::future::join_all;
use nom::{bytes::complete::take_till, character::complete::char, combinator::opt, sequence::pair};
use tokio::{sync::RwLock, task::JoinHandle};
use tracing::info;

use crate::{
    anyhow,
    errors::Result,
    rt_spawn,
    storage::{
        needle_map::NeedleMapType,
        ttl::Ttl,
        volume::{ReplicaPlacement, VolumeRef, DATA_FILE_SUFFIX},
        VolumeError, VolumeId,
    },
};

pub struct DiskLocation {
    pub directory: FastStr,
    pub max_volume_count: i64,
    pub volumes: HashMap<VolumeId, VolumeRef>,
}

impl DiskLocation {
    pub fn new(dir: &str, max_volume_count: i64) -> DiskLocation {
        DiskLocation {
            directory: FastStr::new(dir),
            max_volume_count,
            volumes: HashMap::new(),
        }
    }

    /// concurrent loading volumes
    pub async fn load_existing_volumes(&mut self, needle_map_type: NeedleMapType) -> Result<()> {
        let dir = self.directory.to_string();
        let dir = Path::new(&dir);

        let mut handles: Vec<JoinHandle<Result<(VolumeId, VolumeRef)>>> = vec![];
        for entry in fs::read_dir(dir)? {
            let file = entry?.path();
            let path = file.as_path();

            if path.extension().unwrap_or_default() == DATA_FILE_SUFFIX {
                let (vid, collection) = parse_volume_id_from_path(path)?;
                info!("load volume {} data file {:?}", vid, path);
                if !self.volumes.contains_key(&vid) {
                    let dir = self.directory.clone();
                    let collection = FastStr::new(collection);

                    let handle = rt_spawn(async move {
                        let volume = VolumeRef::new(
                            dir,
                            collection,
                            vid,
                            needle_map_type,
                            ReplicaPlacement::default(),
                            Ttl::default(),
                            0,
                        )
                        .await?;

                        Ok((vid, volume))
                    });
                    handles.push(handle);
                }
            }
        }

        for join in join_all(handles).await {
            let (vid, volume) = join??;
            self.volumes.insert(vid, volume);
        }

        Ok(())
    }

    pub fn add_volume(&mut self, vid: VolumeId, volume: VolumeRef) {
        self.volumes.insert(vid, volume);
    }

    pub fn get_volume(&self, vid: VolumeId) -> Option<VolumeRef> {
        self.volumes.get(&vid).cloned()
    }

    pub fn get_volumes(&self) -> HashMap<VolumeId, VolumeRef> {
        self.volumes.clone()
    }

    pub fn get_volumes_len(&self) -> usize {
        self.volumes.len()
    }

    pub async fn delete_volume(&mut self, vid: VolumeId) -> Result<()> {
        if let Some(v) = self.volumes.remove(&vid) {
            v.read().await.destroy().await?;
            info!(
                "remove volume {vid} success, where disk location is {}",
                self.directory
            );
        }
        Ok(())
    }
}

fn parse_volume_id_from_path(path: &Path) -> StdResult<(VolumeId, &str), VolumeError> {
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

#[derive(Clone)]
pub struct DiskLocationRef(Arc<RwLock<DiskLocation>>);

impl DiskLocationRef {
    pub fn new(location: DiskLocation) -> Self {
        Self(Arc::new(RwLock::new(location)))
    }

    pub async fn read(&self) -> tokio::sync::RwLockReadGuard<'_, DiskLocation> {
        self.0.read().await
    }

    pub async fn write(&self) -> tokio::sync::RwLockWriteGuard<'_, DiskLocation> {
        self.0.write().await
    }
}
