use faststr::FastStr;
use futures::channel::mpsc::unbounded;
use helyim_macros::event_fn;
use helyim_proto::{HeartbeatRequest, VolumeInformationMessage};
use tracing::{debug, error, info, warn};

use crate::{
    anyhow,
    errors::{Error, Result},
    rt_spawn,
    storage::{
        disk_location::{disk_location_loop, DiskLocation, DiskLocationEventTx},
        needle::Needle,
        needle_map::NeedleMapType,
        types::Size,
        volume::{volume_loop, Volume, VolumeEventTx},
        ReplicaPlacement, Ttl, VolumeError, VolumeId,
    },
};

const MAX_TTL_VOLUME_REMOVAL_DELAY_MINUTES: u64 = 10;

pub struct Store {
    pub ip: FastStr,
    pub port: u16,
    pub public_url: FastStr,
    pub locations: Vec<DiskLocationEventTx>,

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

#[event_fn]
impl Store {
    pub fn new(
        ip: &str,
        port: u16,
        public_url: &str,
        folders: Vec<String>,
        max_counts: Vec<i64>,
        needle_map_type: NeedleMapType,
        master_addr: FastStr,
        shutdown: async_broadcast::Receiver<()>,
    ) -> Result<Store> {
        let mut locations = vec![];
        for i in 0..folders.len() {
            let mut location = DiskLocation::new(&folders[i], max_counts[i], shutdown.clone());
            location.load_existing_volumes(needle_map_type)?;

            let (tx, rx) = unbounded();
            rt_spawn(disk_location_loop(location, rx, shutdown.clone()));

            locations.push(DiskLocationEventTx::new(tx));
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

    pub fn locations(&self) -> Vec<DiskLocationEventTx> {
        self.locations.clone()
    }

    pub async fn has_volume(&self, vid: VolumeId) -> Result<bool> {
        Ok(self.find_volume(vid).await?.is_some())
    }

    pub async fn find_volume(&self, vid: VolumeId) -> Result<Option<VolumeEventTx>> {
        for location in self.locations.iter() {
            let volume = location.get_volume(vid).await?;
            if volume.is_some() {
                return Ok(volume);
            }
        }
        Ok(None)
    }

    pub async fn delete_volume_needle(&self, vid: VolumeId, needle: Needle) -> Result<Size> {
        match self.find_volume(vid).await? {
            Some(volume) => volume.delete_needle(needle).await,
            None => Ok(Size(0)),
        }
    }

    pub async fn read_volume_needle(&self, vid: VolumeId, needle: Needle) -> Result<Needle> {
        match self.find_volume(vid).await? {
            Some(volume) => volume.read_needle(needle).await,
            None => Err(VolumeError::NotFound(vid).into()),
        }
    }

    pub async fn write_volume_needle(&self, vid: VolumeId, needle: Needle) -> Result<Needle> {
        match self.find_volume(vid).await? {
            Some(volume) => {
                if volume.is_readonly().await? {
                    return Err(anyhow!("volume {} is read only", vid));
                }

                volume.write_needle(needle).await
            }
            None => Err(VolumeError::NotFound(vid).into()),
        }
    }

    pub async fn delete_volume(&mut self, vid: VolumeId) -> Result<()> {
        let mut delete = false;
        for location in self.locations.iter_mut() {
            location.delete_volume(vid).await?;
            delete = true;
        }
        if delete {
            // TODO: update master
            Ok(())
        } else {
            Err(VolumeError::NotFound(vid).into())
        }
    }

    async fn find_free_location(&self) -> Result<Option<DiskLocationEventTx>> {
        let mut disk_location = None;
        let mut max_free: i64 = 0;
        for location in self.locations.iter() {
            let free =
                location.max_volume_count().await? - location.get_volumes_len().await? as i64;
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
        debug!("do_add_volume vid: {} collection: {}", vid, collection);
        if self.find_volume(vid).await?.is_some() {
            return Err(anyhow!("volume id {} already exists!", vid));
        }

        let location = self
            .find_free_location()
            .await?
            .ok_or::<Error>(anyhow!("no more free space left"))?;

        let volume = Volume::new(
            location.directory().await?,
            collection,
            vid,
            needle_map_type,
            replica_placement,
            ttl,
            preallocate,
        )?;
        let (tx, rx) = unbounded();
        let volume_tx = VolumeEventTx::new(tx);
        rt_spawn(volume_loop(volume, rx, location.shutdown_rx().await?));
        location.add_volume(vid, volume_tx)?;

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
            max_volume_count += location.max_volume_count().await?;
            for (vid, volume) in location.get_volumes().await?.iter() {
                let volume_max_file_key = volume.max_file_key().await?;
                if volume_max_file_key > max_file_key {
                    max_file_key = volume_max_file_key;
                }

                if !volume.expired(self.volume_size_limit).await? {
                    let super_block = volume.super_block().await?;
                    let msg = VolumeInformationMessage {
                        id: *vid,
                        size: volume.size().await.unwrap_or(0),
                        collection: volume.collection().await?.to_string(),
                        file_count: volume.file_count().await?,
                        delete_count: volume.deleted_count().await?,
                        deleted_bytes: volume.deleted_bytes().await?,
                        read_only: volume.is_readonly().await?,
                        replica_placement: Into::<u8>::into(super_block.replica_placement) as u32,
                        version: volume.version().await? as u32,
                        ttl: super_block.ttl.into(),
                    };
                    heartbeat.volumes.push(msg);
                } else if volume
                    .expired_long_enough(MAX_TTL_VOLUME_REMOVAL_DELAY_MINUTES)
                    .await?
                {
                    deleted_vids.push(*vid);
                    info!("volume {} is deleted.", vid);
                } else {
                    info!("volume {} is expired.", vid);
                }
            }
            for vid in deleted_vids {
                if let Err(err) = location.delete_volume(vid).await {
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
                let garbage_level = volume.garbage_level().await?;
                info!("volume {vid} garbage level: {garbage_level}");
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
                volume.compact().await?;
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
                volume.commit_compact().await?;
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
                volume.cleanup_compact().await?;
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
