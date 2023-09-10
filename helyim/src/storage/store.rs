use faststr::FastStr;
use helyim_proto::{HeartbeatRequest, VolumeInformationMessage};
use tracing::{debug, error, info, warn};

use crate::{
    anyhow,
    errors::{Error, Result},
    storage::{
        disk_location::DiskLocation, needle::Needle, needle_map::NeedleMapType, volume::Volume,
        ReplicaPlacement, Ttl, VolumeId,
    },
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
    pub volume_size_limit: u64,
    pub needle_map_type: NeedleMapType,
}

impl Store {
    pub fn new(
        ip: &str,
        port: u16,
        public_url: &str,
        folders: Vec<String>,
        max_counts: Vec<i64>,
        needle_map_type: NeedleMapType,
        shutdown_rx: async_broadcast::Receiver<()>,
    ) -> Result<Store> {
        let mut locations = vec![];
        for i in 0..folders.len() {
            let mut location = DiskLocation::new(&folders[i], max_counts[i], shutdown_rx.clone());
            location.load_existing_volumes(needle_map_type)?;
            locations.push(location);
        }

        Ok(Store {
            ip: FastStr::new(ip),
            port,
            public_url: FastStr::new(public_url),
            locations,
            needle_map_type,
            data_center: FastStr::empty(),
            rack: FastStr::empty(),
            connected: false,
            volume_size_limit: 0,
        })
    }

    pub fn find_volume_mut(&mut self, vid: VolumeId) -> Option<&mut Volume> {
        for location in self.locations.iter_mut() {
            let volume = location.volumes.get_mut(&vid);
            if volume.is_some() {
                return volume;
            }
        }
        None
    }

    pub fn has_volume(&self, vid: VolumeId) -> bool {
        self.find_volume(vid).is_some()
    }

    pub fn find_volume(&self, vid: VolumeId) -> Option<&Volume> {
        for location in self.locations.iter() {
            let volume = location.volumes.get(&vid);
            if volume.is_some() {
                return volume;
            }
        }
        None
    }

    pub async fn delete_volume_needle(&mut self, vid: VolumeId, n: &mut Needle) -> Result<u32> {
        match self.find_volume_mut(vid) {
            Some(v) => v.delete_needle(n).await,
            None => Ok(0),
        }
    }

    pub fn read_volume_needle(&mut self, vid: VolumeId, n: &mut Needle) -> Result<u32> {
        match self.find_volume_mut(vid) {
            Some(v) => v.read_needle(n),
            None => Err(Error::MissingVolume(vid)),
        }
    }

    pub async fn write_volume_needle(&mut self, vid: VolumeId, n: &mut Needle) -> Result<u32> {
        match self.find_volume_mut(vid) {
            Some(v) => {
                if v.read_only {
                    return Err(anyhow!("volume {} is read only", vid));
                }

                v.write_needle(n).await
            }
            None => Err(Error::MissingVolume(vid)),
        }
    }

    pub fn delete_volume(&mut self, vid: VolumeId) -> Result<()> {
        let mut delete = false;
        for location in self.locations.iter_mut() {
            location.delete_volume(vid)?;
            delete = true;
        }
        if delete {
            // TODO: update master
            Ok(())
        } else {
            Err(Error::MissingVolume(vid))
        }
    }

    fn find_free_location(&mut self) -> Option<&mut DiskLocation> {
        let mut disk_location = None;
        let mut max_free: i64 = 0;
        for location in self.locations.iter_mut() {
            let free = location.max_volume_count - location.volumes.len() as i64;
            if free > max_free {
                max_free = free;
                disk_location = Some(location);
            }
        }

        disk_location
    }

    fn do_add_volume(
        &mut self,
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

        let shutdown = location.shutdown_rx.clone();
        let volume = Volume::new(
            location.directory.clone(),
            collection,
            vid,
            needle_map_type,
            replica_placement,
            ttl,
            preallocate,
            shutdown,
        )?;

        location.volumes.insert(vid, volume);

        Ok(())
    }

    pub fn add_volume(
        &mut self,
        volumes: &[u32],
        collection: &str,
        needle_map_type: NeedleMapType,
        replica_placement: &str,
        ttl: &str,
        preallocate: i64,
    ) -> Result<()> {
        let rp = ReplicaPlacement::new(replica_placement)?;
        let ttl = Ttl::new(ttl)?;

        let collection = FastStr::new(collection);
        for volume in volumes {
            self.do_add_volume(
                *volume,
                collection.clone(),
                needle_map_type,
                rp,
                ttl,
                preallocate,
            )?;
        }
        Ok(())
    }

    pub fn collect_heartbeat(&mut self) -> HeartbeatRequest {
        let mut heartbeat = HeartbeatRequest::default();

        let mut max_file_key: u64 = 0;
        let mut max_volume_count = 0;
        for location in self.locations.iter_mut() {
            let mut deleted_vids = Vec::new();
            max_volume_count += location.max_volume_count;
            for (vid, v) in location.volumes.iter() {
                if v.needle_mapper.max_file_key() > max_file_key {
                    max_file_key = v.needle_mapper.max_file_key();
                }

                if !v.expired(self.volume_size_limit) {
                    let msg = VolumeInformationMessage {
                        id: *vid,
                        size: v.size().unwrap_or(0),
                        collection: v.collection.to_string(),
                        file_count: v.needle_mapper.file_count(),
                        delete_count: v.needle_mapper.delete_count(),
                        deleted_bytes: v.needle_mapper.deleted_bytes(),
                        read_only: v.read_only,
                        replica_placement: Into::<u8>::into(v.super_block.replica_placement) as u32,
                        version: v.super_block.version as u32,
                        ttl: v.super_block.ttl.into(),
                    };
                    heartbeat.volumes.push(msg);
                } else if v.expired_long_enough(MAX_TTL_VOLUME_REMOVAL_DELAY_MINUTES) {
                    deleted_vids.push(v.id);
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

        heartbeat
    }
}

/// compact volume
impl Store {
    pub fn check_compact_volume(&self, vid: VolumeId) -> Result<f64> {
        match self.find_volume(vid) {
            Some(volume) => {
                info!("volume {vid} garbage level: {}", volume.garbage_level());
                Ok(volume.garbage_level())
            }
            None => {
                error!("volume {vid} is not found during check compact");
                Err(Error::MissingVolume(vid))
            }
        }
    }

    pub fn compact_volume(&mut self, vid: VolumeId, preallocate: i64) -> Result<()> {
        match self.find_volume_mut(vid) {
            Some(volume) => {
                volume.read_only = true;
                // TODO: check disk status
                volume.compact2(preallocate)?;
                info!("volume {vid} compacting success.");
                Ok(())
            }
            None => {
                error!("volume {vid} is not found during compacting.");
                Err(Error::MissingVolume(vid))
            }
        }
    }

    pub fn commit_compact_volume(&mut self, vid: VolumeId) -> Result<()> {
        match self.find_volume_mut(vid) {
            Some(volume) => {
                // TODO: check disk status
                volume.commit_compact()?;
                volume.read_only = false;
                info!("volume {vid} committing compaction success.");
                Ok(())
            }
            None => {
                error!("volume {vid} is not found during committing compaction.");
                Err(Error::MissingVolume(vid))
            }
        }
    }

    pub fn commit_cleanup_volume(&mut self, vid: VolumeId) -> Result<()> {
        match self.find_volume_mut(vid) {
            Some(volume) => {
                volume.cleanup_compact()?;
                info!("volume {vid} committing cleanup success.");
                Ok(())
            }
            None => {
                error!("volume {vid} is not found during cleaning up.");
                Err(Error::MissingVolume(vid))
            }
        }
    }
}
