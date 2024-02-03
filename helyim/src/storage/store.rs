use std::{
    result::Result as StdResult,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use dashmap::mapref::one::{Ref, RefMut};
use faststr::FastStr;
use helyim_proto::directory::{
    HeartbeatRequest, VolumeInformationMessage, VolumeShortInformationMessage,
};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::{
    anyhow,
    errors::{Error, Result},
    storage::{
        disk_location::DiskLocation,
        needle::{Needle, NeedleMapType, MAX_POSSIBLE_VOLUME_SIZE},
        types::Size,
        volume::Volume,
        ReplicaPlacement, Ttl, VolumeError, VolumeId,
    },
    util::{args::VolumeOptions, chan::DeltaVolumeInfoSender},
};

const MAX_TTL_VOLUME_REMOVAL_DELAY_MINUTES: u64 = 10;

pub struct Store {
    pub ip: FastStr,
    pub port: u16,
    pub public_url: FastStr,
    pub locations: Vec<DiskLocation>,

    pub data_center: FastStr,
    pub rack: FastStr,
    pub connected: bool,

    // read from master
    pub volume_size_limit: AtomicU64,
    pub needle_map_type: NeedleMapType,

    pub delta_volume_tx: DeltaVolumeInfoSender,

    pub current_master: RwLock<FastStr>,
}

impl Store {
    pub async fn new(
        options: Arc<VolumeOptions>,
        needle_map_type: NeedleMapType,
        delta_volume_tx: DeltaVolumeInfoSender,
    ) -> Result<Store> {
        let mut locations = vec![];

        let folders = options.paths();
        let max_counts = options.max_volumes();
        assert_eq!(folders.len(), max_counts.len());

        for i in 0..folders.len() {
            let location = DiskLocation::new(&folders[i], max_counts[i]);
            location.load_existing_volumes(needle_map_type).await?;
            // load erasure coding shards
            location.load_all_shards().await?;
            locations.push(location);
        }

        Ok(Store {
            ip: options.ip.clone(),
            port: options.port,
            public_url: options.public_url(),
            locations,
            needle_map_type,
            data_center: FastStr::empty(),
            rack: FastStr::empty(),
            connected: false,
            volume_size_limit: AtomicU64::new(0),
            delta_volume_tx,
            current_master: RwLock::new(FastStr::empty()),
        })
    }

    pub fn ip(&self) -> FastStr {
        self.ip.clone()
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn volume_size_limit(&self) -> u64 {
        self.volume_size_limit.load(Ordering::Relaxed)
    }

    pub fn set_volume_size_limit(&self, volume_size_limit: u64) {
        self.volume_size_limit
            .store(volume_size_limit, Ordering::Relaxed);
    }

    pub async fn set_current_master(&self, current_master: FastStr) {
        *self.current_master.write().await = current_master;
    }

    pub fn locations(&self) -> &[DiskLocation] {
        self.locations.as_ref()
    }

    pub fn has_volume(&self, vid: VolumeId) -> bool {
        self.find_volume(vid).is_some()
    }

    pub fn find_volume(&self, vid: VolumeId) -> Option<Ref<VolumeId, Volume>> {
        for location in self.locations.iter() {
            let volume = location.get_volume(vid);
            if volume.is_some() {
                return volume;
            }
        }
        None
    }

    pub fn find_volume_mut(&self, vid: VolumeId) -> Option<RefMut<VolumeId, Volume>> {
        for location in self.locations.iter() {
            let volume = location.get_volume_mut(vid);
            if volume.is_some() {
                return volume;
            }
        }
        None
    }

    pub async fn delete_volume_needle(
        &self,
        vid: VolumeId,
        needle: &mut Needle,
    ) -> StdResult<usize, VolumeError> {
        match self.find_volume(vid) {
            Some(volume) => {
                if volume.no_write_or_delete() {
                    return Err(VolumeError::Readonly(vid));
                }
                if MAX_POSSIBLE_VOLUME_SIZE >= volume.content_size() + Size(0).actual_size() {
                    return volume.delete_needle(needle);
                }
                Err(VolumeError::VolumeSizeLimit(
                    self.volume_size_limit(),
                    volume.content_size(),
                ))
            }
            None => Ok(0),
        }
    }

    pub async fn read_volume_needle(&self, vid: VolumeId, needle: &mut Needle) -> Result<usize> {
        match self.find_volume(vid) {
            Some(volume) => Ok(volume.read_needle(needle)?),
            None => Err(VolumeError::NotFound(vid).into()),
        }
    }

    pub async fn write_volume_needle(&self, vid: VolumeId, needle: &mut Needle) -> Result<usize> {
        match self.find_volume(vid) {
            Some(volume) => {
                if volume.readonly() {
                    return Err(VolumeError::Readonly(vid).into());
                }

                Ok(volume.write_needle(needle)?)
            }
            None => Err(VolumeError::NotFound(vid).into()),
        }
    }

    pub async fn delete_volume(&self, vid: VolumeId) -> Result<()> {
        let volume = self.find_volume(vid);
        if volume.is_none() {
            return Ok(());
        }
        let volume = volume.unwrap();

        for location in self.locations.iter() {
            if location.delete_volume(vid).is_ok() {
                self.delta_volume_tx
                    .delete_volume(VolumeShortInformationMessage {
                        id: *volume.key(),
                        collection: volume.collection.to_string(),
                        replica_placement: Into::<u8>::into(volume.super_block.replica_placement)
                            as u32,
                        version: volume.version() as u32,
                        ttl: volume.super_block.ttl.to_u32(),
                    })
                    .await;
                return Ok(());
            }
        }
        Err(VolumeError::NotFound(vid).into())
    }

    pub async fn mark_volume_readonly(&self, volume_id: VolumeId) -> StdResult<(), VolumeError> {
        match self.find_volume(volume_id) {
            Some(volume) => {
                volume.set_no_write_or_delete(true);
                Ok(())
            }
            None => Err(VolumeError::NotFound(volume_id)),
        }
    }

    async fn find_free_location(&self) -> Result<Option<&DiskLocation>> {
        let mut disk_location = None;
        let mut max_free: i64 = 0;
        for location in self.locations.iter() {
            let free = location.max_volume_count - location.volumes.len() as i64;
            if free > max_free {
                max_free = free;
                disk_location = Some(location);
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
        if self.find_volume(vid).is_some() {
            return Err(anyhow!("volume id {} already exists!", vid));
        }

        let location = self
            .find_free_location()
            .await?
            .ok_or::<Error>(anyhow!("no more free space left"))?;

        let volume = Volume::new(
            location.directory.clone(),
            collection.clone(),
            vid,
            needle_map_type,
            replica_placement,
            ttl,
            preallocate,
        )?;

        let version = volume.version();
        location.add_volume(vid, volume);

        self.delta_volume_tx
            .add_volume(VolumeShortInformationMessage {
                id: vid,
                collection: collection.to_string(),
                replica_placement: Into::<u8>::into(replica_placement) as u32,
                version: version as u32,
                ttl: ttl.to_u32(),
            })
            .await;
        Ok(())
    }

    pub async fn add_volume(
        &self,
        volume_id: VolumeId,
        collection: String,
        needle_map_type: NeedleMapType,
        replica_placement: String,
        ttl: String,
        preallocate: i64,
    ) -> Result<()> {
        let rp = ReplicaPlacement::new(&replica_placement)?;
        let ttl = Ttl::new(&ttl)?;

        let collection = FastStr::new(collection);
        self.do_add_volume(
            volume_id,
            collection.clone(),
            needle_map_type,
            rp,
            ttl,
            preallocate,
        )
        .await?;
        Ok(())
    }

    pub fn collect_heartbeat(&self) -> Result<HeartbeatRequest> {
        let mut heartbeat = HeartbeatRequest::default();

        let mut max_file_key: u64 = 0;
        let mut max_volume_count = 0;
        for location in self.locations.iter() {
            let mut deleted_vids = Vec::new();
            max_volume_count += location.max_volume_count;
            for volume in location.volumes.iter() {
                let vid = volume.key();
                let volume_max_file_key = volume.max_file_key();
                if volume_max_file_key > max_file_key {
                    max_file_key = volume_max_file_key;
                }

                if !volume.expired(self.volume_size_limit()) {
                    let rp: u8 = volume.super_block.replica_placement.into();
                    let msg = VolumeInformationMessage {
                        id: *vid,
                        size: volume.data_file_size().unwrap_or(0),
                        collection: volume.collection.to_string(),
                        file_count: volume.file_count(),
                        delete_count: volume.deleted_count(),
                        deleted_bytes: volume.deleted_bytes(),
                        read_only: volume.no_write_or_delete(),
                        replica_placement: rp as u32,
                        version: volume.version() as u32,
                        ttl: volume.super_block.ttl.into(),
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
        heartbeat.has_no_volumes = heartbeat.volumes.is_empty();
        heartbeat.has_no_ec_shards = heartbeat.ec_shards.is_empty();

        Ok(heartbeat)
    }

    pub fn check_compact_volume(&self, vid: VolumeId) -> Result<f64> {
        match self.find_volume(vid) {
            Some(volume) => {
                let garbage_level = volume.garbage_level();
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

    pub fn compact_volume(&self, vid: VolumeId, _preallocate: u64) -> Result<()> {
        match self.find_volume(vid) {
            Some(volume) => {
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

    pub fn commit_compact_volume(&self, vid: VolumeId) -> Result<()> {
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

pub type StoreRef = Arc<Store>;

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
