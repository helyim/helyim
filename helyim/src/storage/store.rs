use std::sync::Arc;

use faststr::FastStr;
use helyim_proto::{HeartbeatRequest, VolumeInformationMessage};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::{
    anyhow,
    errors::{Error, Result},
    storage::{
        disk_location::{DiskLocation, DiskLocationRef},
        needle::Needle,
        needle_map::NeedleMapType,
        types::Size,
        volume::VolumeRef,
        ReplicaPlacement, Ttl, VolumeError, VolumeId,
    },
};

const MAX_TTL_VOLUME_REMOVAL_DELAY_MINUTES: u64 = 10;

pub struct Store {
    pub ip: FastStr,
    pub port: u16,
    pub public_url: FastStr,
    pub locations: Vec<DiskLocationRef>,

    pub data_center: FastStr,
    pub rack: FastStr,
    pub connected: bool,

    pub master_addr: FastStr,
    // read from master
    pub volume_size_limit: u64,
    pub needle_map_type: NeedleMapType,
}

unsafe impl Send for Store {}

unsafe impl Sync for Store {}

impl Store {
    pub async fn new(
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
            location.load_existing_volumes(needle_map_type).await?;

            locations.push(DiskLocationRef::new(location));
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
            volume_size_limit: 0,
        })
    }

    pub fn ip(&self) -> FastStr {
        self.ip.clone()
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn set_volume_size_limit(&mut self, volume_size_limit: u64) {
        self.volume_size_limit = volume_size_limit;
    }

    pub fn locations(&self) -> Vec<DiskLocationRef> {
        self.locations.clone()
    }

    pub async fn has_volume(&self, vid: VolumeId) -> Result<bool> {
        Ok(self.find_volume(vid).await?.is_some())
    }

    pub async fn find_volume(&self, vid: VolumeId) -> Result<Option<VolumeRef>> {
        for location in self.locations.iter() {
            let volume = location.read().await.get_volume(vid);
            if volume.is_some() {
                return Ok(volume);
            }
        }
        Ok(None)
    }

    pub async fn delete_volume_needle(&self, vid: VolumeId, needle: Needle) -> Result<Size> {
        match self.find_volume(vid).await? {
            Some(volume) => Ok(volume.write().await.delete_needle(needle).await?),
            None => Ok(Size(0)),
        }
    }

    pub async fn read_volume_needle(&self, vid: VolumeId, needle: Needle) -> Result<Needle> {
        match self.find_volume(vid).await? {
            Some(volume) => Ok(volume.write().await.read_needle(needle).await?),
            None => Err(VolumeError::NotFound(vid).into()),
        }
    }

    pub async fn write_volume_needle(&self, vid: VolumeId, needle: Needle) -> Result<Needle> {
        match self.find_volume(vid).await? {
            Some(volume) => {
                if volume.read().await.is_readonly() {
                    return Err(VolumeError::Readonly(vid).into());
                }

                Ok(volume.write().await.write_needle(needle).await?)
            }
            None => Err(VolumeError::NotFound(vid).into()),
        }
    }

    pub async fn delete_volume(&mut self, vid: VolumeId) -> Result<()> {
        let mut delete = false;
        for location in self.locations.iter_mut() {
            location.write().await.delete_volume(vid).await?;
            delete = true;
        }
        if delete {
            // TODO: update master
            Ok(())
        } else {
            Err(VolumeError::NotFound(vid).into())
        }
    }

    async fn find_free_location(&self) -> Result<Option<DiskLocationRef>> {
        let mut disk_location = None;
        let mut max_free: i64 = 0;
        for location in self.locations.iter() {
            let free = location.read().await.max_volume_count
                - location.read().await.get_volumes_len() as i64;
            if free > max_free {
                max_free = free;
                disk_location = Some(location.clone());
            }
        }

        Ok(disk_location)
    }

    async fn do_add_volume(
        &self,
        vid: VolumeId,
        collection: FastStr,
        needle_map_type: NeedleMapType,
        replica_placement: ReplicaPlacement,
        ttl: Ttl,
        preallocate: i64,
    ) -> Result<()> {
        debug!(
            "add volume: {}, collection: {}, ttl: {}, replica placement: {}",
            vid, collection, ttl, replica_placement
        );
        if self.find_volume(vid).await?.is_some() {
            return Err(anyhow!("volume id {} already exists!", vid));
        }

        let location = self
            .find_free_location()
            .await?
            .ok_or::<Error>(anyhow!("no more free space left"))?;

        let volume = VolumeRef::new(
            location.read().await.directory.clone(),
            collection,
            vid,
            needle_map_type,
            replica_placement,
            ttl,
            preallocate,
        )
        .await?;
        location.write().await.add_volume(vid, volume);

        Ok(())
    }

    pub async fn add_volume(
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
            )
            .await?;
        }
        Ok(())
    }

    pub async fn collect_heartbeat(&mut self) -> Result<HeartbeatRequest> {
        let mut heartbeat = HeartbeatRequest::default();

        let mut max_file_key: u64 = 0;
        let mut max_volume_count = 0;
        for location in self.locations.iter_mut() {
            let mut deleted_vids = Vec::new();
            max_volume_count += location.read().await.max_volume_count;
            for (vid, volume) in location.read().await.get_volumes().iter() {
                let volume_max_file_key = volume.read().await.max_file_key();
                if volume_max_file_key > max_file_key {
                    max_file_key = volume_max_file_key;
                }

                if !volume.read().await.expired(self.volume_size_limit) {
                    let super_block = volume.read().await.super_block;
                    let msg = VolumeInformationMessage {
                        id: *vid,
                        size: volume.read().await.size().await.unwrap_or(0),
                        collection: volume.read().await.collection.to_string(),
                        file_count: volume.read().await.file_count(),
                        delete_count: volume.read().await.deleted_count(),
                        deleted_bytes: volume.read().await.deleted_bytes(),
                        read_only: volume.read().await.is_readonly(),
                        replica_placement: Into::<u8>::into(super_block.replica_placement) as u32,
                        version: volume.read().await.version() as u32,
                        ttl: super_block.ttl.into(),
                    };
                    heartbeat.volumes.push(msg);
                } else if volume
                    .read()
                    .await
                    .expired_long_enough(MAX_TTL_VOLUME_REMOVAL_DELAY_MINUTES)
                {
                    deleted_vids.push(*vid);
                    info!("volume {} is deleted.", vid);
                } else {
                    info!("volume {} is expired.", vid);
                }
            }
            for vid in deleted_vids {
                if let Err(err) = location.write().await.delete_volume(vid).await {
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

    pub async fn check_compact_volume(&self, vid: VolumeId) -> Result<f64> {
        match self.find_volume(vid).await? {
            Some(volume) => {
                let garbage_level = volume.read().await.garbage_level();
                if garbage_level > 0.0 {
                    info!("volume {vid} garbage level: {garbage_level}");
                }
                Ok(garbage_level)
            }
            None => {
                error!("volume {vid} is not found during check compact");
                Err(VolumeError::NotFound(vid).into())
            }
        }
    }

    pub async fn compact_volume(&self, vid: VolumeId, _preallocate: u64) -> Result<()> {
        match self.find_volume(vid).await? {
            Some(volume) => {
                // TODO: check disk status
                volume.write().await.compact2().await?;
                info!("volume {vid} compacting success.");
                Ok(())
            }
            None => {
                error!("volume {vid} is not found during compacting.");
                Err(VolumeError::NotFound(vid).into())
            }
        }
    }

    pub async fn commit_compact_volume(&self, vid: VolumeId) -> Result<()> {
        match self.find_volume(vid).await? {
            Some(volume) => {
                // TODO: check disk status
                volume.write().await.commit_compact().await?;
                info!("volume {vid} committing compaction success.");
                Ok(())
            }
            None => {
                error!("volume {vid} is not found during committing compaction.");
                Err(VolumeError::NotFound(vid).into())
            }
        }
    }

    pub async fn commit_cleanup_volume(&self, vid: VolumeId) -> Result<()> {
        match self.find_volume(vid).await? {
            Some(volume) => {
                volume.write().await.cleanup_compact()?;
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

#[derive(Clone)]
pub struct StoreRef(Arc<RwLock<Store>>);

impl StoreRef {
    pub async fn new(
        ip: &str,
        port: u16,
        public_url: &str,
        folders: Vec<String>,
        max_counts: Vec<i64>,
        needle_map_type: NeedleMapType,
        master_addr: FastStr,
    ) -> Result<Self> {
        Ok(Self(Arc::new(RwLock::new(
            Store::new(
                ip,
                port,
                public_url,
                folders,
                max_counts,
                needle_map_type,
                master_addr,
            )
            .await?,
        ))))
    }

    pub async fn read(&self) -> tokio::sync::RwLockReadGuard<'_, Store> {
        self.0.read().await
    }

    pub async fn write(&self) -> tokio::sync::RwLockWriteGuard<'_, Store> {
        self.0.write().await
    }
}
