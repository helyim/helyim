use std::{fs, path::Path};

use dashmap::{
    mapref::one::{Ref, RefMut},
    DashMap,
};
use faststr::FastStr;
use futures::future::join_all;
use nom::{bytes::complete::take_till, character::complete::char, combinator::opt, sequence::pair};
use tokio::task::JoinHandle;
use tracing::info;

use crate::{
    anyhow, rt_spawn,
    storage::{
        erasure_coding::EcVolumeRef,
        needle::NeedleMapType,
        ttl::Ttl,
        volume::{ReplicaPlacement, Volume, DATA_FILE_SUFFIX},
        VolumeError, VolumeId,
    },
};

pub struct DiskLocation {
    pub directory: FastStr,
    pub max_volume_count: i64,
    pub volumes: DashMap<VolumeId, Volume>,
    pub ec_volumes: DashMap<VolumeId, EcVolumeRef>,
}

impl DiskLocation {
    pub fn new(dir: &str, max_volume_count: i64) -> DiskLocation {
        DiskLocation {
            directory: FastStr::new(dir),
            max_volume_count,
            volumes: DashMap::new(),
            ec_volumes: DashMap::new(),
        }
    }

    /// concurrent loading volumes
    pub async fn load_existing_volumes(
        &self,
        needle_map_type: NeedleMapType,
    ) -> Result<(), VolumeError> {
        let dir = self.directory.to_string();
        let dir = Path::new(&dir);

        #[allow(clippy::type_complexity)]
        let mut handles: Vec<JoinHandle<Result<(VolumeId, Volume), VolumeError>>> = vec![];
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
                        let volume = Volume::new(
                            dir,
                            collection,
                            vid,
                            needle_map_type,
                            ReplicaPlacement::default(),
                            Ttl::default(),
                            0,
                        )?;

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

    pub fn add_volume(&self, vid: VolumeId, volume: Volume) {
        self.volumes.insert(vid, volume);
    }

    pub fn get_volume(&self, vid: VolumeId) -> Option<Ref<VolumeId, Volume>> {
        self.volumes.get(&vid)
    }

    pub fn get_volume_mut(&self, vid: VolumeId) -> Option<RefMut<VolumeId, Volume>> {
        self.volumes.get_mut(&vid)
    }

    pub async fn delete_volume(&self, vid: VolumeId) -> Result<(), VolumeError> {
        if let Some((vid, v)) = self.volumes.remove(&vid) {
            v.destroy()?;
            info!(
                "remove volume {vid} success, where disk location is {}",
                self.directory
            );
        }
        Ok(())
    }
}

fn parse_volume_id_from_path(path: &Path) -> Result<(VolumeId, &str), VolumeError> {
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
