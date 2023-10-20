use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use dashmap::mapref::one::{Ref, RefMut};
use faststr::FastStr;
use helyim_proto::{HeartbeatRequest, VolumeInformationMessage};
use tracing::{debug, error, info, warn};

use crate::{
    anyhow,
    errors::{Error, Result},
    storage::{
        disk_location::DiskLocation, needle::Needle, needle_map::NeedleMapType, types::Size,
        volume::Volume, ReplicaPlacement, Ttl, VolumeError, VolumeId,
    },
};

const MAX_TTL_VOLUME_REMOVAL_DELAY_MINUTES: u64 = 10;

pub struct Store {
    pub ip: FastStr,
    pub port: u16,
    pub public_url: FastStr,
    pub locations: Vec<Arc<DiskLocation>>,

    pub data_center: FastStr,
    pub rack: FastStr,
    pub connected: bool,

    pub master_addr: FastStr,
    // read from master
    pub volume_size_limit: AtomicU64,
    pub needle_map_type: NeedleMapType,
}

unsafe impl Send for Store {}
unsafe impl Sync for Store {}

impl Store {
    pub fn new(
        ip: &str,
        port: u16,
        public_url: &str,
        folders: Vec<String>,
        max_counts: Vec<i64>,
        needle_map_type: NeedleMapType,
        master_addr: FastStr,
    ) -> Result<Store> {
        let mut locations = vec![];
        for i in 0..folders.len() {
            let mut location = DiskLocation::new(&folders[i], max_counts[i]);
            location.load_existing_volumes(needle_map_type)?;
            locations.push(Arc::new(location));
        }

        Ok(Store {
            ip: FastStr::new(ip),
            port,
            public_url: FastStr::new(public_url),
            locations,
            needle_map_type,
            master_addr,
            data_center: FastStr::empty(),
            rack: FastStr::empty(),
            connected: false,
            volume_size_limit: AtomicU64::default(),
        })
    }

    pub fn has_volume(&self, vid: VolumeId) -> bool {
        self.find_volume(vid).is_some()
    }

    pub fn find_volume(&self, vid: VolumeId) -> Option<Ref<VolumeId, Volume>> {
        for location in self.locations.iter() {
            let volume = location.volumes.get(&vid);
            if volume.is_some() {
                return volume;
            }
        }
        None
    }

    pub fn find_volume_mut(&mut self, vid: VolumeId) -> Option<RefMut<VolumeId, Volume>> {
        for location in self.locations.iter_mut() {
            let volume = location.volumes.get_mut(&vid);
            if volume.is_some() {
                return volume;
            }
        }
        None
    }

    pub async fn delete_volume_needle(&mut self, vid: VolumeId, needle: Needle) -> Result<Size> {
        match self.find_volume_mut(vid) {
            Some(mut volume) => volume.delete_needle(needle).await,
            None => Ok(Size(0)),
        }
    }

    pub fn read_volume_needle(&self, vid: VolumeId, needle: Needle) -> Result<Needle> {
        match self.find_volume(vid) {
            Some(volume) => volume.read_needle(needle),
            None => Err(VolumeError::NotFound(vid).into()),
        }
    }

    pub async fn write_volume_needle(&mut self, vid: VolumeId, needle: &mut Needle) -> Result<()> {
        match self.find_volume_mut(vid) {
            Some(mut volume) => {
                if volume.is_readonly() {
                    return Err(anyhow!("volume {} is read only", vid));
                }

                volume.write_needle(needle).await
            }
            None => Err(VolumeError::NotFound(vid).into()),
        }
    }

    pub fn delete_volume(&self, vid: VolumeId) -> Result<()> {
        let mut delete = false;
        for location in self.locations.iter() {
            location.delete_volume(vid)?;
            delete = true;
        }
        if delete {
            // TODO: update master
            Ok(())
        } else {
            Err(VolumeError::NotFound(vid).into())
        }
    }

    fn find_free_location(&self) -> Option<&Arc<DiskLocation>> {
        let mut disk_location = None;
        let mut max_free: i64 = 0;
        for location in self.locations.iter() {
            let free = location.max_volume_count - location.volumes.len() as i64;
            if free > max_free {
                max_free = free;
                disk_location = Some(location);
            }
        }

        disk_location
    }

    fn do_add_volume(
        &self,
        vid: VolumeId,
        collection: FastStr,
        needle_map_type: NeedleMapType,
        replica_placement: ReplicaPlacement,
        ttl: Ttl,
        preallocate: i64,
    ) -> Result<()> {
        debug!("do_add_volume vid: {} collection: {}", vid, collection);
        if self.find_volume(vid).is_some() {
            return Err(anyhow!("volume id {} already exists!", vid));
        }

        let location = self
            .find_free_location()
            .ok_or::<Error>(anyhow!("no more free space left"))?;

        let volume = Volume::new(
            location.directory.clone(),
            collection,
            vid,
            needle_map_type,
            replica_placement,
            ttl,
            preallocate,
        )?;
        location.volumes.insert(vid, volume);

        Ok(())
    }

    pub fn add_volume(
        &self,
        volumes: Vec<u32>,
        collection: String,
        needle_map_type: NeedleMapType,
        replica_placement: String,
        ttl: String,
        preallocate: i64,
    ) -> Result<()> {
        let rp = ReplicaPlacement::new(&replica_placement)?;
        let ttl = Ttl::new(&ttl)?;

        let collection = FastStr::new(collection);
        for volume in volumes {
            self.do_add_volume(
                volume,
                collection.clone(),
                needle_map_type,
                rp,
                ttl,
                preallocate,
            )?;
        }
        Ok(())
    }

    pub async fn collect_heartbeat(&self) -> Result<HeartbeatRequest> {
        let mut heartbeat = HeartbeatRequest::default();

        let mut max_file_key: u64 = 0;
        let mut max_volume_count = 0;
        for location in self.locations.iter() {
            let mut deleted_vids = Vec::new();
            max_volume_count += location.max_volume_count;
            for entry in location.volumes.iter() {
                let vid = entry.key();
                let volume = entry.value();
                let volume_max_file_key = volume.max_file_key();
                if volume_max_file_key > max_file_key {
                    max_file_key = volume_max_file_key;
                }

                if !volume.expired(self.volume_size_limit.load(Ordering::Relaxed)) {
                    let super_block = volume.super_block;
                    let msg = VolumeInformationMessage {
                        id: *vid,
                        size: volume.size().unwrap_or(0),
                        collection: volume.collection.to_string(),
                        file_count: volume.file_count(),
                        delete_count: volume.deleted_count(),
                        deleted_bytes: volume.deleted_bytes(),
                        read_only: volume.is_readonly(),
                        replica_placement: Into::<u8>::into(super_block.replica_placement) as u32,
                        version: volume.version() as u32,
                        ttl: super_block.ttl.into(),
                    };
                    heartbeat.volumes.push(msg);
                } else if volume.expired_long_enough(MAX_TTL_VOLUME_REMOVAL_DELAY_MINUTES) {
                    deleted_vids.push(*vid);
                    info!("volume {} is deleted.", vid);
                } else {
                    info!("volume {} is expired.", vid);
                }
            }
            for vid in deleted_vids {
                if let Err(err) = location.delete_volume(vid) {
                    warn!("delete volume {vid} err: {err}");
                }
            }
        }

        heartbeat.ip = self.ip.to_string();
        heartbeat.port = self.port as u32;
        heartbeat.public_url = self.public_url.to_string();
        heartbeat.max_volume_count = max_volume_count as u32;
        heartbeat.max_file_key = max_file_key;
        heartbeat.data_center = self.data_center.to_string();
        heartbeat.rack = self.rack.to_string();

        Ok(heartbeat)
    }

    pub fn check_compact_volume(&self, vid: VolumeId) -> Result<f64> {
        match self.find_volume(vid) {
            Some(volume) => {
                let garbage_level = volume.garbage_level();
                info!("volume {vid} garbage level: {garbage_level}");
                Ok(garbage_level)
            }
            None => {
                error!("volume {vid} is not found during check compact");
                Err(VolumeError::NotFound(vid).into())
            }
        }
    }

    pub fn compact_volume(&mut self, vid: VolumeId, _preallocate: u64) -> Result<()> {
        match self.find_volume_mut(vid) {
            Some(mut volume) => {
                // TODO: check disk status
                volume.compact()?;
                info!("volume {vid} compacting success.");
                Ok(())
            }
            None => {
                error!("volume {vid} is not found during compacting.");
                Err(VolumeError::NotFound(vid).into())
            }
        }
    }

    pub fn commit_compact_volume(&mut self, vid: VolumeId) -> Result<()> {
        match self.find_volume_mut(vid) {
            Some(mut volume) => {
                // TODO: check disk status
                volume.commit_compact()?;
                info!("volume {vid} committing compaction success.");
                Ok(())
            }
            None => {
                error!("volume {vid} is not found during committing compaction.");
                Err(VolumeError::NotFound(vid).into())
            }
        }
    }

    pub fn commit_cleanup_volume(&self, vid: VolumeId) -> Result<()> {
        match self.find_volume(vid) {
            Some(volume) => {
                volume.cleanup_compact()?;
                info!("volume {vid} committing cleanup success.");
                Ok(())
            }
            None => {
                error!("volume {vid} is not found during cleaning up.");
                Err(VolumeError::NotFound(vid).into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::{channel::mpsc::channel, SinkExt, StreamExt};
    use tokio::time::timeout;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_async_scope() {
        let timeout = timeout(Duration::from_secs(1), async {
            let (tx, mut rx) = channel(10);

            let handle = tokio::spawn(async move {
                while let Some(i) = rx.next().await {
                    println!("{i}");
                }
            });

            async_scoped::TokioScope::scope_and_block(|s| {
                for i in 0..10 {
                    let mut tmp_tx = tx.clone();
                    s.spawn(async move {
                        let _ = tmp_tx.send(i).await;
                    });
                }
            });

            drop(tx);

            match handle.await {
                Ok(_) => println!("done"),
                Err(err) => eprintln!("{err}"),
            }
        })
        .await;

        if let Err(err) = timeout {
            panic!("{err}");
        }
    }
}
