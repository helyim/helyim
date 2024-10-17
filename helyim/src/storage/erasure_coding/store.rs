use std::{
    io::ErrorKind,
    ops::Add,
    os::unix::fs::FileExt,
    sync::Arc,
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use dashmap::mapref::one::Ref;
use faststr::FastStr;
use futures::{channel::mpsc::channel, SinkExt, StreamExt};
use helyim_common::types::{NeedleId, VolumeId};
use helyim_proto::{
    directory::{HeartbeatRequest, LookupEcVolumeRequest, VolumeEcShardInformationMessage},
    volume::VolumeEcShardReadRequest,
};
use reed_solomon_erasure::{galois_8::Field, ReedSolomon};
use tracing::{error, info};

use crate::{
    storage::{
        erasure_coding::{
            add_shard_id, volume::EcVolume, EcShardError, EcVolumeError, EcVolumeShard, Interval,
            ShardId, DATA_SHARDS_COUNT, ERASURE_CODING_LARGE_BLOCK_SIZE,
            ERASURE_CODING_SMALL_BLOCK_SIZE, PARITY_SHARDS_COUNT, TOTAL_SHARDS_COUNT,
        },
        store::Store,
        Needle, NeedleError,
    },
    util::grpc::{helyim_client, volume_server_client},
};

impl Store {
    pub async fn find_ec_shard(
        &self,
        vid: VolumeId,
        shard_id: ShardId,
    ) -> Option<Arc<EcVolumeShard>> {
        for location in self.locations.iter() {
            let shard = location.find_ec_shard(vid, shard_id).await;
            if shard.is_some() {
                return shard;
            }
        }
        None
    }

    pub fn has_ec_volume(&self, vid: VolumeId) -> bool {
        self.find_ec_volume(vid).is_some()
    }

    pub fn find_ec_volume(&self, vid: VolumeId) -> Option<Ref<VolumeId, EcVolume>> {
        for location in self.locations.iter() {
            if let Some(volume) = location.ec_volumes.get(&vid) {
                return Some(volume);
            }
        }
        None
    }

    pub async fn destroy_ec_volume(&self, vid: VolumeId) -> Result<(), EcVolumeError> {
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
    ) -> Result<(), EcVolumeError> {
        for location in self.locations.iter() {
            match location.load_ec_shard(&collection, vid, shard_id).await {
                Ok(_) => {
                    self.delta_volume_tx
                        .add_ec_shard(VolumeEcShardInformationMessage {
                            id: vid,
                            collection: collection.to_string(),
                            ec_index_bits: add_shard_id(0, shard_id),
                        })
                        .await;
                    return Ok(());
                }
                Err(EcVolumeError::Io(err)) => {
                    if err.kind() == ErrorKind::NotFound {
                        continue;
                    }
                    error!(
                        "{} load ec shard {vid}.{shard_id}, error: {err}",
                        location.directory
                    );
                    return Err(EcVolumeError::Io(err));
                }
                Err(err) => {
                    error!(
                        "{} load ec shard {vid}.{shard_id}, error: {err}",
                        location.directory
                    );
                    return Err(err);
                }
            }
        }
        Err(EcVolumeError::ShardNotFound(vid, shard_id))
    }

    pub async fn unmount_ec_shards(
        &self,
        vid: VolumeId,
        shard_id: ShardId,
    ) -> Result<(), EcVolumeError> {
        if let Some(shard) = self.find_ec_shard(vid, shard_id).await {
            for location in self.locations.iter() {
                if location.unload_ec_shard(vid, shard_id).await {
                    self.delta_volume_tx
                        .delete_ec_shard(VolumeEcShardInformationMessage {
                            id: vid,
                            collection: shard.collection.to_string(),
                            ec_index_bits: add_shard_id(0, shard_id),
                        })
                        .await;
                }
            }
        }
        Err(EcVolumeError::ShardNotFound(vid, shard_id))
    }

    pub async fn read_ec_shard_needle(
        &self,
        vid: VolumeId,
        needle: &mut Needle,
    ) -> Result<usize, EcVolumeError> {
        for location in self.locations.iter() {
            if let Some(volume) = location.find_ec_volume(vid) {
                let (index, intervals) = volume
                    .locate_ec_shard_needle(needle.id, volume.version)
                    .await?;
                if index.size.is_deleted() {
                    return Err(EcVolumeError::Needle(NeedleError::Deleted(vid, needle.id)));
                }

                info!(
                    "locate needle within ec shard success, ec volume: {vid}, offset: {}, size: \
                     {}, interval len: {}",
                    index.offset.actual_offset(),
                    index.size.actual_size(),
                    intervals.len()
                );

                let (bytes, is_deleted) = self
                    .read_ec_shard_intervals(needle.id, volume.value(), intervals)
                    .await?;
                if is_deleted {
                    return Err(EcVolumeError::Needle(NeedleError::Deleted(vid, needle.id)));
                }

                let bytes = Bytes::from(bytes);
                let len = bytes.len();
                info!(
                    "read ec shard intervals success, volume id: {vid}, needle id: {}, read {len} \
                     bytes ",
                    needle.id
                );

                needle.read_bytes(bytes, index.offset, index.size, volume.version)?;
                return Ok(len);
            }
        }
        Err(EcVolumeError::ShardNotFound(vid, 0))
    }

    async fn cached_lookup_ec_shard_locations(
        &self,
        volume: &EcVolume,
    ) -> Result<(), EcVolumeError> {
        let shard_count = volume.shard_locations.len() as u32;
        let now = SystemTime::now();
        if shard_count < DATA_SHARDS_COUNT
            && volume
                .shard_locations_refresh_time
                .read()
                .await
                .add(Duration::from_secs(11))
                > now
            || shard_count == TOTAL_SHARDS_COUNT
                && volume
                    .shard_locations_refresh_time
                    .read()
                    .await
                    .add(Duration::from_secs(37 * 60))
                    > now
            || shard_count >= DATA_SHARDS_COUNT
                && volume
                    .shard_locations_refresh_time
                    .read()
                    .await
                    .add(Duration::from_secs(7 * 60))
                    > now
        {
            return Ok(());
        }

        info!("lookup and cache ec volume {} locations", volume.volume_id);

        let master = self.current_master.read().await.clone();
        let client = helyim_client(&master)?;
        let request = LookupEcVolumeRequest {
            volume_id: volume.volume_id,
        };
        let response = client.lookup_ec_volume(request).await?;
        let response = response.into_inner();
        if response.shard_id_locations.len() < DATA_SHARDS_COUNT as usize {
            error!(
                "only {} shards found but {} required",
                response.shard_id_locations.len(),
                DATA_SHARDS_COUNT
            );
            return Err(EcShardError::Underflow(
                response.shard_id_locations.len(),
                DATA_SHARDS_COUNT as usize,
            )
            .into());
        }

        for location in response.shard_id_locations {
            let shard_id = location.shard_id as ShardId;
            volume.shard_locations.remove(&shard_id);
            for loc in location.locations {
                if let Some(mut entry) = volume.shard_locations.get_mut(&shard_id) {
                    entry.push(FastStr::new(loc.url));
                }
            }
        }

        *volume.shard_locations_refresh_time.write().await = SystemTime::now();
        Ok(())
    }

    async fn read_remote_ec_shard_interval(
        &self,
        src_nodes: &[FastStr],
        needle_id: NeedleId,
        volume_id: VolumeId,
        shard_id: ShardId,
        buf: &mut [u8],
        offset: u64,
    ) -> Result<(usize, bool), EcVolumeError> {
        if src_nodes.is_empty() {
            return Err(EcVolumeError::ShardNotFound(volume_id, shard_id));
        }

        for src_node in src_nodes {
            match self
                .do_read_remote_ec_shard_interval(
                    src_node, needle_id, volume_id, shard_id, buf, offset,
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
        src_node: &str,
        needle_id: NeedleId,
        volume_id: VolumeId,
        shard_id: ShardId,
        buf: &mut [u8],
        offset: u64,
    ) -> Result<(usize, bool), EcVolumeError> {
        let client = volume_server_client(src_node)?;
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

    async fn read_ec_shard_intervals(
        &self,
        needle_id: NeedleId,
        volume: &EcVolume,
        intervals: Vec<Interval>,
    ) -> Result<(Vec<u8>, bool), EcVolumeError> {
        self.cached_lookup_ec_shard_locations(volume).await?;

        let mut data = Vec::new();
        let mut is_deleted = false;
        for (i, interval) in intervals.iter().enumerate() {
            let (dat, deleted) = self
                .read_one_ec_shard_interval(needle_id, volume, interval)
                .await?;
            if deleted {
                is_deleted = true;
            }
            if i == 0 {
                data = dat;
            } else {
                data.extend_from_slice(&dat);
            }
        }

        Ok((data, is_deleted))
    }

    async fn read_one_ec_shard_interval(
        &self,
        needle_id: NeedleId,
        ec_volume: &EcVolume,
        interval: &Interval,
    ) -> Result<(Vec<u8>, bool), EcVolumeError> {
        let actual_offset = interval.offset(
            ERASURE_CODING_LARGE_BLOCK_SIZE,
            ERASURE_CODING_SMALL_BLOCK_SIZE,
        );
        let shard_id = interval.shard_id();

        let mut data = vec![0u8; interval.size as usize];

        match ec_volume.find_ec_shard(shard_id).await {
            Some(shard) => {
                info!(
                    "read local ec shard success, shard id: {shard_id}, actual offset: \
                     {actual_offset}"
                );
                shard.ecd_file.read_exact_at(&mut data, actual_offset)?;
                Ok((data, false))
            }
            None => {
                if let Some(locations) = ec_volume.shard_locations.get(&shard_id) {
                    match self
                        .read_remote_ec_shard_interval(
                            &locations,
                            needle_id,
                            ec_volume.volume_id,
                            shard_id,
                            &mut data,
                            actual_offset,
                        )
                        .await
                    {
                        Ok((_, is_deleted)) => {
                            info!(
                                "read remote ec shard interval success, volume: {}, needle id: \
                                 {needle_id}, shard id: {shard_id}, actual offset: {actual_offset}",
                                ec_volume.volume_id
                            );
                            return Ok((data, is_deleted));
                        }
                        Err(err) => {
                            info!(
                                "clearing ec shard {}.{shard_id} locations: {err}",
                                ec_volume.volume_id
                            );
                            ec_volume.shard_locations.remove(&shard_id);
                        }
                    }
                }

                let (_, is_deleted) = self
                    .recover_one_remote_ec_shard_interval(
                        needle_id,
                        ec_volume,
                        shard_id,
                        &mut data,
                        actual_offset,
                    )
                    .await?;
                info!(
                    "recover one remote ec shard interval success, volume: {}, needle id: \
                     {needle_id}, shard id: {shard_id}, actual offset: {actual_offset}",
                    ec_volume.volume_id
                );
                Ok((data, is_deleted))
            }
        }
    }

    async fn recover_one_remote_ec_shard_interval(
        &self,
        needle_id: NeedleId,
        ec_volume: &EcVolume,
        shard_id_to_recover: ShardId,
        buf: &mut [u8],
        offset: u64,
    ) -> Result<(usize, bool), EcVolumeError> {
        let reed_solomon: ReedSolomon<Field> =
            ReedSolomon::new(DATA_SHARDS_COUNT as usize, PARITY_SHARDS_COUNT as usize)?;
        let mut bufs = vec![None; TOTAL_SHARDS_COUNT as usize];

        let (tx, mut rx) = channel(ec_volume.shard_locations.len());

        let reconstruct = tokio::spawn(async move {
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
            for shard_location in ec_volume.shard_locations.iter() {
                let shard_id = *shard_location.key();
                let locations = shard_location.value().clone();
                if shard_id == shard_id_to_recover {
                    continue;
                }
                if locations.is_empty() {
                    continue;
                }

                let buf_len = buf.len();
                let mut entry_tx = tx.clone();
                s.spawn(async move {
                    let mut data = vec![0; buf_len];
                    match self
                        .read_remote_ec_shard_interval(
                            &locations,
                            needle_id,
                            ec_volume.volume_id,
                            shard_id,
                            &mut data,
                            offset,
                        )
                        .await
                    {
                        Ok((read, is_deleted)) => {
                            if read == buf_len {
                                if let Err(err) = entry_tx.send((shard_id, data, is_deleted)).await
                                {
                                    error!(
                                        "read from remote locations failed, needle_id: \
                                         {needle_id}, shard_id: {shard_id}, error: {err}"
                                    );
                                }
                            }
                        }
                        Err(err) => {
                            error!("read ec shard from remote location error: {err}");
                            let shard = shard_id as ShardId;
                            ec_volume.shard_locations.remove(&shard);
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

    pub async fn collect_erasure_coding_heartbeat(&self) -> HeartbeatRequest {
        let mut ec_shard_messages = Vec::new();

        for location in self.locations.iter() {
            for ec_shard in location.ec_volumes.iter() {
                ec_shard_messages.extend(ec_shard.get_volume_ec_shard_info().await);
            }
        }

        HeartbeatRequest {
            has_no_ec_shards: ec_shard_messages.is_empty(),
            ec_shards: ec_shard_messages,
            ..Default::default()
        }
    }
}
