use std::{io::ErrorKind, sync::Arc};

use faststr::FastStr;
use futures::{
    channel::mpsc::{channel, unbounded, Sender},
    SinkExt, StreamExt,
};
use helyim_macros::event_fn;
use helyim_proto::{
    volume_server_client::VolumeServerClient, HeartbeatRequest, VolumeEcShardInformationMessage,
    VolumeEcShardReadRequest, VolumeInformationMessage, VolumeShortInformationMessage,
};
use reed_solomon_erasure::{galois_8::Field, ReedSolomon};
use tracing::{debug, error, info, warn};

use crate::{
    anyhow,
    errors::{Error, Result},
    rt_spawn,
    storage::{
        disk_location::DiskLocation,
        erasure_coding::{
            add_shard_id, EcVolume, EcVolumeEventTx, EcVolumeShard, ShardId, DATA_SHARDS_COUNT,
            PARITY_SHARDS_COUNT, TOTAL_SHARDS_COUNT,
        },
        needle::Needle,
        needle_map::NeedleMapType,
        types::Size,
        volume::{volume_loop, Volume, VolumeEventTx},
        ErasureCodingError, NeedleId, ReplicaPlacement, Ttl, VolumeError, VolumeId,
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
    // read from master
    pub volume_size_limit: u64,
    pub needle_map_type: NeedleMapType,

    new_volumes_tx: Option<Sender<VolumeShortInformationMessage>>,
    deleted_volumes_tx: Option<Sender<VolumeShortInformationMessage>>,
    new_ec_shards_tx: Option<Sender<VolumeEcShardInformationMessage>>,
    deleted_ec_shards_tx: Option<Sender<VolumeEcShardInformationMessage>>,
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
        shutdown: async_broadcast::Receiver<()>,
    ) -> Result<Store> {
        let mut locations = vec![];
        for i in 0..folders.len() {
            let mut location = DiskLocation::new(&folders[i], max_counts[i], shutdown.clone());
            location.load_existing_volumes(needle_map_type)?;
            locations.push(Arc::new(location));
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
            new_volumes_tx: None,
            deleted_volumes_tx: None,
            new_ec_shards_tx: None,
            deleted_ec_shards_tx: None,
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

    pub fn locations(&self) -> Vec<Arc<DiskLocation>> {
        self.locations.clone()
    }

    pub fn has_volume(&self, vid: VolumeId) -> bool {
        self.find_volume(vid).is_some()
    }

    pub fn find_volume(&self, vid: VolumeId) -> Option<VolumeEventTx> {
        for location in self.locations.iter() {
            let volume = location.volumes.get(&vid);
            if volume.is_some() {
                return volume.map(|v| v.value().clone());
            }
        }
        None
    }

    pub async fn delete_volume_needle(&self, vid: VolumeId, needle: Needle) -> Result<Size> {
        match self.find_volume(vid) {
            Some(volume) => volume.delete_needle(needle).await,
            None => Ok(Size(0)),
        }
    }

    pub async fn read_volume_needle(&self, vid: VolumeId, needle: Needle) -> Result<Needle> {
        match self.find_volume(vid) {
            Some(volume) => volume.read_needle(needle).await,
            None => Err(VolumeError::NotFound(vid).into()),
        }
    }

    pub async fn write_volume_needle(&self, vid: VolumeId, needle: Needle) -> Result<Needle> {
        match self.find_volume(vid) {
            Some(volume) => {
                if volume.is_readonly().await? {
                    return Err(anyhow!("volume {} is read only", vid));
                }

                volume.write_needle(needle).await
            }
            None => Err(VolumeError::NotFound(vid).into()),
        }
    }

    pub async fn delete_volume(&self, vid: VolumeId) -> Result<()> {
        let mut delete = false;
        for location in self.locations.iter() {
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
        let (tx, rx) = unbounded();
        let volume_tx = VolumeEventTx::new(tx);
        rt_spawn(volume_loop(volume, rx, location.shutdown.clone()));
        location.volumes.insert(vid, volume_tx);

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

    pub fn set_event_tx(
        &mut self,
        new_volumes_tx: Sender<VolumeShortInformationMessage>,
        deleted_volumes_tx: Sender<VolumeShortInformationMessage>,
        new_ec_shards_tx: Sender<VolumeEcShardInformationMessage>,
        deleted_ec_shards_tx: Sender<VolumeEcShardInformationMessage>,
    ) {
        self.new_volumes_tx = Some(new_volumes_tx);
        self.deleted_volumes_tx = Some(deleted_volumes_tx);
        self.new_ec_shards_tx = Some(new_ec_shards_tx);
        self.deleted_ec_shards_tx = Some(deleted_ec_shards_tx);
    }

    pub async fn collect_heartbeat(&mut self) -> Result<HeartbeatRequest> {
        let mut heartbeat = HeartbeatRequest::default();

        let mut max_file_key: u64 = 0;
        let mut max_volume_count = 0;
        for location in self.locations.iter_mut() {
            let mut deleted_vids = Vec::new();
            max_volume_count += location.max_volume_count;
            for entry in location.volumes.iter() {
                let vid = entry.key();
                let volume = entry.value();
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
        match self.find_volume(vid) {
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
        match self.find_volume(vid) {
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
        match self.find_volume(vid) {
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
        match self.find_volume(vid) {
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

    // erasure coding
    pub async fn find_ec_shard(
        &self,
        vid: VolumeId,
        shard_id: ShardId,
    ) -> Result<Option<Arc<EcVolumeShard>>> {
        for location in self.locations.iter() {
            let shard = location.find_ec_shard(vid, shard_id).await?;
            if shard.is_some() {
                return Ok(shard);
            }
        }
        Ok(None)
    }

    pub fn find_ec_volume(&self, vid: VolumeId) -> Option<EcVolumeEventTx> {
        for location in self.locations.iter() {
            let volume = location.ec_volumes.get(&vid);
            if volume.is_some() {
                return volume.map(|v| v.value().clone());
            }
        }
        None
    }

    pub async fn destroy_ec_volume(&mut self, vid: VolumeId) -> Result<()> {
        for location in self.locations.iter() {
            location.destroy_ec_volume(vid).await?;
        }
        Ok(())
    }

    pub async fn mount_ec_shards(
        &mut self,
        collection: String,
        vid: VolumeId,
        shard_id: ShardId,
    ) -> Result<()> {
        for location in self.locations.iter_mut() {
            match location.load_ec_shard(&collection, vid, shard_id).await {
                Ok(_) => {
                    if let Some(tx) = self.new_ec_shards_tx.as_mut() {
                        let shard_bits = 0;
                        tx.send(VolumeEcShardInformationMessage {
                            id: vid,
                            collection: collection.to_string(),
                            ec_index_bits: add_shard_id(shard_bits, shard_id),
                        })
                        .await?;
                    }
                    return Ok(());
                }
                Err(Error::Io(err)) => {
                    if err.kind() == ErrorKind::NotFound {
                        continue;
                    }
                }
                Err(err) => {
                    error!("mount ec shard failed, error: {err}");
                    return Err(err);
                }
            }
        }
        Ok(())
    }

    pub async fn unmount_ec_shards(&mut self, vid: VolumeId, shard_id: ShardId) -> Result<()> {
        match self.find_ec_shard(vid, shard_id).await? {
            Some(shard) => {
                for location in self.locations.iter_mut() {
                    if location.unload_ec_shard(vid, shard_id).await? {
                        if let Some(tx) = self.deleted_ec_shards_tx.as_mut() {
                            let shard_bits = 0;
                            tx.send(VolumeEcShardInformationMessage {
                                id: vid,
                                collection: shard.collection.to_string(),
                                ec_index_bits: add_shard_id(shard_bits, shard_id),
                            })
                            .await?;
                        }
                        break;
                    }
                }
                Ok(())
            }
            None => Err(ErasureCodingError::ShardNotFound(vid, shard_id).into()),
        }
    }

    async fn read_remote_ec_shard_interval(
        &self,
        src_nodes: &[FastStr],
        needle_id: NeedleId,
        volume_id: VolumeId,
        shard_id: ShardId,
        buf: &mut [u8],
        offset: u64,
    ) -> Result<(usize, bool)> {
        if src_nodes.is_empty() {
            return Err(ErasureCodingError::ShardNotFound(volume_id, shard_id).into());
        }

        for src_node in src_nodes {
            match self
                .do_read_remote_ec_shard_interval(
                    src_node.to_string(),
                    needle_id,
                    volume_id,
                    shard_id,
                    buf,
                    offset,
                )
                .await
            {
                Ok((bytes_read, is_deleted)) => {
                    return Ok((bytes_read, is_deleted));
                }
                Err(err) => {
                    error!("read remote ec shard {volume_id}.{shard_id} from {src_node}, {err}")
                }
            }
        }

        Ok((0, false))
    }

    async fn do_read_remote_ec_shard_interval(
        &self,
        src_node: String,
        needle_id: NeedleId,
        volume_id: VolumeId,
        shard_id: ShardId,
        buf: &mut [u8],
        offset: u64,
    ) -> Result<(usize, bool)> {
        let mut client = VolumeServerClient::connect(src_node).await?;
        let response = client
            .volume_ec_shard_read(VolumeEcShardReadRequest {
                volume_id,
                shard_id: shard_id as u32,
                offset: offset as i64,
                size: buf.len() as i64,
                file_key: needle_id,
            })
            .await?;
        let mut response = response.into_inner();
        let mut is_deleted = false;
        let mut bytes_read = 0;
        while let Some(Ok(response)) = response.next().await {
            if response.is_deleted {
                is_deleted = true;
            }
            let len = response.data.len();
            buf[bytes_read..bytes_read + len].copy_from_slice(&response.data);
            bytes_read += len;
        }
        Ok((bytes_read, is_deleted))
    }

    async fn read_from_remote_locations(
        &self,
        locations: &[FastStr],
        volume: &EcVolume,
        shard_id: ShardId,
        needle_id: NeedleId,
        offset: u64,
        buf_len: usize,
    ) -> Result<(Option<Vec<u8>>, bool)> {
        let mut data = vec![0u8; buf_len];
        let (bytes_read, is_deleted) = match self
            .read_remote_ec_shard_interval(
                locations,
                needle_id,
                volume.volume_id,
                shard_id,
                &mut data,
                offset,
            )
            .await
        {
            Ok((bytes_read, is_deleted)) => (bytes_read, is_deleted),
            Err(err) => {
                volume.shard_locations.remove(&shard_id);
                (0, false)
            }
        };
        let data = if bytes_read == buf_len {
            Some(data)
        } else {
            None
        };

        Ok((data, is_deleted))
    }

    async fn recover_one_remote_ec_shard_interval(
        &self,
        needle_id: NeedleId,
        ec_volume: &EcVolume,
        shard_id_to_recover: ShardId,
        buf: &mut [u8],
        offset: u64,
    ) -> Result<(usize, bool)> {
        let reed_solomon: ReedSolomon<Field> =
            ReedSolomon::new(DATA_SHARDS_COUNT as usize, PARITY_SHARDS_COUNT as usize)?;
        let mut bufs = vec![None; TOTAL_SHARDS_COUNT as usize];

        let (tx, mut rx) = channel(ec_volume.shard_locations.len());

        let reconstruct = rt_spawn(async move {
            let mut is_deleted = false;
            while let Some((shard_id, data, if_deleted)) = rx.next().await {
                bufs[shard_id as usize] = Some(data);
                if if_deleted {
                    is_deleted = if_deleted;
                }
            }

            let ret = match reed_solomon.reconstruct(&mut bufs) {
                Ok(_) => Ok(bufs),
                Err(err) => Err(err),
            };

            (ret, is_deleted)
        });

        async_scoped::TokioScope::scope_and_block(|s| {
            for entry in ec_volume.shard_locations.iter() {
                let shard_id = *entry.key();
                if shard_id == shard_id_to_recover {
                    continue;
                }
                if entry.value().is_empty() {
                    continue;
                }

                let buf_len = buf.len();
                let mut entry_tx = tx.clone();
                s.spawn(async move {
                    if let Ok((Some(data), is_deleted)) = self
                        .read_from_remote_locations(
                            entry.value(),
                            ec_volume,
                            shard_id,
                            needle_id,
                            offset,
                            buf_len,
                        )
                        .await
                    {
                        if let Err(err) = entry_tx.send((shard_id, data, is_deleted)).await {
                            error!(
                                "read from remote locations failed, needle_id: {needle_id}, \
                                 shard_id: {shard_id}, error: {err}"
                            );
                        }
                    }
                });
            }
        });

        // prevent reconstruct thread from staying alive indefinitely
        drop(tx);

        let (data, is_deleted) = reconstruct.await?;
        if let Some(Some(data)) = data?.get(shard_id_to_recover as usize) {
            buf.copy_from_slice(data);
        }
        Ok((buf.len(), is_deleted))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::{channel::mpsc::channel, SinkExt, StreamExt};
    use tokio::time::timeout;

    use crate::rt_spawn;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    pub async fn test_async_scope() {
        let timeout = timeout(Duration::from_secs(1), async {
            let (tx, mut rx) = channel(10);

            let handle = rt_spawn(async move {
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
