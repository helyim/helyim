use std::{collections::HashMap, time::Duration};

use faststr::FastStr;
use futures::{
    channel::mpsc::{unbounded, UnboundedReceiver},
    StreamExt,
};
use helyim_macros::event_fn;
use helyim_proto::{
    VacuumVolumeCheckRequest, VacuumVolumeCleanupRequest, VacuumVolumeCommitRequest,
    VacuumVolumeCompactRequest,
};
use serde::Serialize;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::{
    errors::Result,
    rt_spawn,
    sequence::{Sequence, Sequencer},
    storage::{FileId, ReplicaPlacement, Ttl, VolumeId, VolumeInfo},
    topology::{
        collection::Collection, data_center_loop, volume_grow::VolumeGrowOption,
        volume_layout::VolumeLayout, DataCenter, DataCenterEventTx, DataNodeEventTx,
    },
};

#[derive(Serialize)]
pub struct Topology {
    #[serde(skip)]
    sequencer: Sequencer,
    pub collections: HashMap<FastStr, Collection>,
    pulse: u64,
    volume_size_limit: u64,
    #[serde(skip)]
    data_centers: HashMap<FastStr, DataCenterEventTx>,
    #[serde(skip)]
    handles: Vec<JoinHandle<()>>,
    #[serde(skip)]
    shutdown: async_broadcast::Receiver<()>,
}

unsafe impl Send for Topology {}

impl Clone for Topology {
    fn clone(&self) -> Self {
        Self {
            sequencer: self.sequencer.clone(),
            collections: self.collections.clone(),
            pulse: self.pulse,
            volume_size_limit: self.volume_size_limit,
            data_centers: HashMap::new(),
            handles: Vec::new(),
            shutdown: self.shutdown.clone(),
        }
    }
}

impl Topology {
    fn get_volume_layout(
        &mut self,
        collection: FastStr,
        rp: ReplicaPlacement,
        ttl: Ttl,
    ) -> &mut VolumeLayout {
        self.collections
            .entry(collection.clone())
            .or_insert(Collection::new(collection, self.volume_size_limit))
            .get_or_create_volume_layout(rp, Some(ttl))
    }

    async fn get_max_volume_id(&self) -> Result<VolumeId> {
        let mut vid = 0;
        for (_, dc_tx) in self.data_centers.iter() {
            let other = dc_tx.max_volume_id().await?;
            if other > vid {
                vid = other;
            }
        }

        Ok(vid)
    }
}

#[event_fn]
impl Topology {
    pub fn new(
        sequencer: Sequencer,
        volume_size_limit: u64,
        pulse: u64,
        shutdown: async_broadcast::Receiver<()>,
    ) -> Topology {
        Topology {
            sequencer,
            collections: HashMap::new(),
            pulse,
            volume_size_limit,
            data_centers: HashMap::new(),
            handles: Vec::new(),
            shutdown,
        }
    }

    pub fn get_or_create_data_center(&mut self, name: FastStr) -> DataCenterEventTx {
        self.data_centers
            .entry(name.clone())
            .or_insert_with(|| {
                let (tx, rx) = unbounded();
                self.handles.push(rt_spawn(data_center_loop(
                    DataCenter::new(name, self.shutdown.clone()),
                    rx,
                )));

                DataCenterEventTx::new(tx)
            })
            .clone()
    }

    pub fn lookup(
        &mut self,
        collection: FastStr,
        volume_id: VolumeId,
    ) -> Option<Vec<DataNodeEventTx>> {
        if collection.is_empty() {
            for c in self.collections.values() {
                let data_node = c.lookup(volume_id);
                if data_node.is_some() {
                    return data_node;
                }
            }
        } else if let Some(c) = self.collections.get(&collection) {
            let data_node = c.lookup(volume_id);
            if data_node.is_some() {
                return data_node;
            }
        }

        None
    }

    pub async fn has_writable_volume(&mut self, option: VolumeGrowOption) -> Result<bool> {
        let vl = self.get_volume_layout(
            option.collection.clone(),
            option.replica_placement,
            option.ttl,
        );

        Ok(vl.active_volume_count(option).await? > 0)
    }

    pub async fn free_volumes(&self) -> Result<i64> {
        let mut free = 0;
        for dc_tx in self.data_centers.values() {
            free += dc_tx.max_volumes().await? - dc_tx.has_volumes().await?;
        }
        Ok(free)
    }

    pub async fn pick_for_write(
        &mut self,
        count: u64,
        option: VolumeGrowOption,
    ) -> Result<(FileId, u64, DataNodeEventTx)> {
        let (volume_id, nodes) = {
            let layout = self.get_volume_layout(
                option.collection.clone(),
                option.replica_placement,
                option.ttl,
            );
            layout.pick_for_write(&option).await?
        };

        let file_id = self.sequencer.next_file_id(count)?;

        let file_id = FileId {
            volume_id,
            key: file_id,
            hash: rand::random::<u32>(),
        };
        Ok((file_id, count, nodes[0].clone()))
    }

    pub async fn register_volume_layout(
        &mut self,
        volume: VolumeInfo,
        data_node: DataNodeEventTx,
    ) -> Result<()> {
        self.get_volume_layout(
            volume.collection.clone(),
            volume.replica_placement,
            volume.ttl,
        )
        .register_volume(&volume, data_node)
        .await
    }

    pub async fn unregister_volume_layout(&mut self, volume: VolumeInfo) {
        self.get_volume_layout(
            volume.collection.clone(),
            volume.replica_placement,
            volume.ttl,
        )
        .unregister_volume(&volume);
    }

    pub async fn next_volume_id(&mut self) -> Result<VolumeId> {
        let vid = self.get_max_volume_id().await?;

        Ok(vid + 1)
    }

    pub fn set_max_sequence(&mut self, seq: u64) {
        self.sequencer.set_max(seq);
    }

    pub fn data_centers(&self) -> HashMap<FastStr, DataCenterEventTx> {
        self.data_centers.clone()
    }

    pub fn topology(&self) -> Topology {
        self.clone()
    }

    pub async fn vacuum(&self, garbage_threshold: f64, preallocate: u64) -> Result<()> {
        for (_name, collection) in self.collections.iter() {
            for (_key, volume_layout) in collection.volume_layouts.iter() {
                let location = volume_layout.locations.clone();
                for (vid, data_nodes) in location {
                    if volume_layout.readonly_volumes.contains(&vid) {
                        continue;
                    }

                    if batch_vacuum_volume_check(vid, &data_nodes, garbage_threshold).await?
                        && batch_vacuum_volume_compact(volume_layout, vid, &data_nodes, preallocate)
                            .await?
                    {
                        batch_vacuum_volume_commit(volume_layout, vid, &data_nodes).await?;
                        // let _ = batch_vacuum_volume_cleanup(vid, data_nodes).await;
                    }
                }
            }
        }

        Ok(())
    }
}

pub async fn topology_loop(
    mut topology: Topology,
    mut topology_rx: UnboundedReceiver<TopologyEvent>,
) {
    info!("topology event loop starting.");
    loop {
        tokio::select! {
            Some(event) = topology_rx.next() => {
                match event {
                    TopologyEvent::GetOrCreateDataCenter{name, tx} => {
                        let _ = tx.send(topology.get_or_create_data_center(name));
                    }
                    TopologyEvent::Lookup {
                        collection,
                        volume_id,
                        tx,
                    } => {
                        let _ = tx.send(topology.lookup(collection, volume_id));
                    }
                    TopologyEvent::HasWritableVolume{option, tx} => {
                        let _ = tx.send(topology.has_writable_volume(option).await);
                    }
                    TopologyEvent::FreeVolumes{tx} => {
                        let _ = tx.send(topology.free_volumes().await);
                    }
                    TopologyEvent::PickForWrite { count, option, tx } => {
                        let _ = tx.send(topology.pick_for_write(count, option).await);
                    }
                    TopologyEvent::RegisterVolumeLayout {
                        volume,
                        data_node,
                        tx,
                    } => {
                        let _ = tx.send(
                            topology
                                .register_volume_layout(volume, data_node)
                                .await,
                        );
                    }
                    TopologyEvent::UnregisterVolumeLayout{volume} => {
                        topology.unregister_volume_layout(volume).await;
                    }
                    TopologyEvent::NextVolumeId{tx} => {
                        let _ = tx.send(topology.next_volume_id().await);
                    }
                    TopologyEvent::DataCenters{tx} => {
                        let _ = tx.send(topology.data_centers());
                    }
                    TopologyEvent::SetMaxSequence{seq} => {
                        topology.set_max_sequence(seq);
                    }
                    TopologyEvent::Topology{tx} => {
                        let _ = tx.send(topology.topology());
                    }
                    TopologyEvent::Vacuum {
                        garbage_threshold,
                        preallocate,
                        tx,
                    } => {
                        let _ = tx.send(topology.vacuum(garbage_threshold, preallocate).await);
                    }
                }
            }
            _ = topology.shutdown.recv() => {
                break;
            }
        }
    }
    info!("topology event loop stopped.");
}

pub async fn topology_vacuum_loop(
    topology: TopologyEventTx,
    garbage_threshold: f64,
    preallocate: u64,
    mut shutdown: async_broadcast::Receiver<()>,
) {
    info!("topology vacuum loop starting");
    let mut interval = tokio::time::interval(Duration::from_secs(60));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                info!("topology vacuum starting.");
                match topology.vacuum(garbage_threshold, preallocate).await {
                    Ok(_) => info!("topology vacuum success."),
                    Err(err) => error!("topology vacuum failed, {err}")
                }
            }
            _ = shutdown.recv() => {
                break;
            }
        }
    }
    info!("topology vacuum loop stopped")
}

async fn batch_vacuum_volume_check(
    volume_id: VolumeId,
    data_nodes: &[DataNodeEventTx],
    garbage_ratio: f64,
) -> Result<bool> {
    let mut check_success = true;
    for data_node_tx in data_nodes {
        let request = VacuumVolumeCheckRequest { volume_id };
        match data_node_tx.vacuum_volume_check(request).await {
            Ok(response) => {
                info!("check volume {volume_id} success.");
                check_success = response.garbage_ratio > garbage_ratio;
            }
            Err(err) => {
                error!("check volume {volume_id} failed, {err}");
                check_success = false;
            }
        }
    }
    Ok(check_success)
}

async fn batch_vacuum_volume_compact(
    volume_layout: &VolumeLayout,
    volume_id: VolumeId,
    data_nodes: &[DataNodeEventTx],
    preallocate: u64,
) -> Result<bool> {
    volume_layout.remove_from_writable(volume_id);
    let mut compact_success = true;
    for data_node_tx in data_nodes {
        let request = VacuumVolumeCompactRequest {
            volume_id,
            preallocate,
        };
        match data_node_tx.vacuum_volume_compact(request).await {
            Ok(_) => {
                info!("compact volume {volume_id} success.");
                compact_success = true;
            }
            Err(err) => {
                error!("compact volume {volume_id} failed, {err}");
                compact_success = false;
            }
        }
    }
    Ok(compact_success)
}

async fn batch_vacuum_volume_commit(
    volume_layout: &VolumeLayout,
    volume_id: VolumeId,
    data_nodes: &[DataNodeEventTx],
) -> Result<bool> {
    let mut commit_success = true;
    for data_node_tx in data_nodes {
        let request = VacuumVolumeCommitRequest { volume_id };
        match data_node_tx.vacuum_volume_commit(request).await {
            Ok(response) => {
                if response.is_read_only {
                    warn!("volume {volume_id} is read only, will not commit it.");
                    commit_success = false;
                } else {
                    info!("commit volume {volume_id} success.");
                    commit_success = true;
                    volume_layout
                        .set_volume_available(volume_id, data_node_tx)
                        .await?;
                }
            }
            Err(err) => {
                error!("commit volume {volume_id} failed, {err}");
                commit_success = false;
            }
        }
    }
    Ok(commit_success)
}

#[allow(dead_code)]
async fn batch_vacuum_volume_cleanup(
    volume_id: VolumeId,
    data_nodes: &[DataNodeEventTx],
) -> Result<bool> {
    let mut cleanup_success = true;
    for data_node_tx in data_nodes {
        let request = VacuumVolumeCleanupRequest { volume_id };
        match data_node_tx.vacuum_volume_cleanup(request).await {
            Ok(_) => {
                info!("cleanup volume {volume_id} success.");
                cleanup_success = true;
            }
            Err(_err) => {
                cleanup_success = false;
            }
        }
    }
    Ok(cleanup_success)
}
