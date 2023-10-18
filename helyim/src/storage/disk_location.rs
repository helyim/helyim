use std::{fs, path::Path};

use dashmap::DashMap;
use faststr::FastStr;
use nom::{bytes::complete::take_till, character::complete::char, combinator::opt, sequence::pair};
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
    pub volumes: DashMap<VolumeId, Volume>,
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
                    entry.insert(volume);
                }
            }
        }

        Ok(())
    }

    pub fn delete_volume(&self, vid: VolumeId) -> Result<()> {
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
