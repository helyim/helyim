use std::{
    collections::HashMap,
    result::Result as StdResult,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Weak,
    },
    time::Duration,
};

use faststr::FastStr;
use serde::Serialize;
use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::{
    raft::{NodeId, Raft},
    sequence::{Sequence, Sequencer},
    storage::{
        batch_vacuum_volume_check, batch_vacuum_volume_commit, batch_vacuum_volume_compact, FileId,
        ReplicaPlacement, Ttl, VolumeError, VolumeId, VolumeInfo,
    },
    topology::{
        collection::Collection, data_center::DataCenterRef, erasure_coding::EcShardLocations,
        volume_grow::VolumeGrowOption, volume_layout::VolumeLayoutRef, DataNodeRef,
    },
};

#[derive(Serialize)]
pub struct Topology {
    #[serde(skip)]
    sequencer: Sequencer,
    pub collections: HashMap<FastStr, Collection>,
    #[serde(skip)]
    pub ec_shards: HashMap<VolumeId, EcShardLocations>,
    pub ec_shard_count: AtomicU64,
    pulse: u64,
    volume_size_limit: u64,
    // children
    #[serde(skip)]
    pub(crate) data_centers: HashMap<FastStr, DataCenterRef>,

    #[serde(skip)]
    pub raft: Option<Raft>,
}

impl Clone for Topology {
    fn clone(&self) -> Self {
        Self {
            sequencer: self.sequencer.clone(),
            collections: self.collections.clone(),
            ec_shards: self.ec_shards.clone(),
            ec_shard_count: AtomicU64::new(self.ec_shard_count.load(Ordering::Relaxed)),
            pulse: self.pulse,
            volume_size_limit: self.volume_size_limit,
            data_centers: HashMap::new(),
            raft: None,
        }
    }
}

impl Topology {
    pub fn new(sequencer: Sequencer, volume_size_limit: u64, pulse: u64) -> Topology {
        Topology {
            sequencer,
            collections: HashMap::new(),
            ec_shards: HashMap::new(),
            ec_shard_count: AtomicU64::new(0),
            pulse,
            volume_size_limit,
            data_centers: HashMap::new(),
            raft: None,
        }
    }

    pub async fn get_or_create_data_center(&mut self, name: FastStr) -> DataCenterRef {
        match self.data_centers.get(&name) {
            Some(data_node) => data_node.clone(),
            None => {
                let data_center = DataCenterRef::new(name.clone());
                self.data_centers.insert(name, data_center.clone());
                data_center
            }
        }
    }

    pub async fn lookup(
        &mut self,
        collection: FastStr,
        volume_id: VolumeId,
    ) -> Option<Vec<DataNodeRef>> {
        if collection.is_empty() {
            for c in self.collections.values() {
                let data_node = c.lookup(volume_id).await;
                if data_node.is_some() {
                    return data_node;
                }
            }
        } else if let Some(c) = self.collections.get(&collection) {
            let data_node = c.lookup(volume_id).await;
            if data_node.is_some() {
                return data_node;
            }
        }

        None
    }

    pub async fn has_writable_volume(&mut self, option: Arc<VolumeGrowOption>) -> bool {
        let vl = self.get_volume_layout(
            option.collection.clone(),
            option.replica_placement,
            option.ttl,
        );

        vl.read().await.active_volume_count(option).await > 0
    }

    pub async fn free_volumes(&self) -> i64 {
        let mut free = 0;
        for data_center in self.data_centers.values() {
            free += data_center.read().await.max_volumes().await
                - data_center.read().await.has_volumes().await;
        }
        free
    }

    pub async fn pick_for_write(
        &mut self,
        count: u64,
        option: Arc<VolumeGrowOption>,
    ) -> StdResult<(FileId, u64, DataNodeRef), VolumeError> {
        let file_id = self
            .sequencer
            .next_file_id(count)
            .map_err(|err| VolumeError::Box(Box::new(err)))?;

        let (volume_id, node) = {
            let layout = self.get_volume_layout(
                option.collection.clone(),
                option.replica_placement,
                option.ttl,
            );
            let layout = layout.read().await;
            let (vid, nodes) = layout.pick_for_write(option.as_ref()).await?;
            (vid, nodes[0].clone())
        };

        let file_id = FileId::new(volume_id, file_id, rand::random::<u32>());
        Ok((file_id, count, node))
    }

    pub async fn register_volume_layout(&mut self, volume: VolumeInfo, data_node: DataNodeRef) {
        self.get_volume_layout(
            volume.collection.clone(),
            volume.replica_placement,
            volume.ttl,
        )
        .write()
        .await
        .register_volume(&volume, data_node)
        .await
    }

    pub async fn unregister_volume_layout(&mut self, volume: VolumeInfo) {
        self.get_volume_layout(
            volume.collection.clone(),
            volume.replica_placement,
            volume.ttl,
        )
        .write()
        .await
        .unregister_volume(&volume);
    }

    pub async fn next_volume_id(&self) -> VolumeId {
        let vid = self.get_max_volume_id().await;

        vid + 1
    }

    pub fn set_max_sequence(&mut self, seq: u64) {
        self.sequencer.set_max(seq);
    }

    pub fn topology(&self) -> Topology {
        self.clone()
    }

    pub async fn vacuum(&mut self, garbage_threshold: f64, preallocate: u64) {
        for (_name, collection) in self.collections.iter_mut() {
            for (_key, volume_layout) in collection.volume_layouts.iter_mut() {
                // TODO: avoid cloning the HashMap
                let locations = volume_layout.read().await.locations.clone();
                for (vid, data_nodes) in locations {
                    if volume_layout
                        .read()
                        .await
                        .readonly_volumes
                        .contains_key(&vid)
                    {
                        continue;
                    }

                    if batch_vacuum_volume_check(vid, &data_nodes, garbage_threshold).await
                        && batch_vacuum_volume_compact(volume_layout, vid, &data_nodes, preallocate)
                            .await
                    {
                        batch_vacuum_volume_commit(volume_layout, vid, &data_nodes).await;
                        // let _ = batch_vacuum_volume_cleanup(vid, data_nodes).await;
                    }
                }
            }
        }
    }
}

impl Topology {
    fn get_volume_layout(
        &mut self,
        collection: FastStr,
        rp: ReplicaPlacement,
        ttl: Ttl,
    ) -> &mut VolumeLayoutRef {
        self.collections
            .entry(collection.clone())
            .or_insert(Collection::new(collection, self.volume_size_limit))
            .get_or_create_volume_layout(rp, Some(ttl))
    }

    async fn get_max_volume_id(&self) -> VolumeId {
        let mut vid = 0;
        for (_, data_center) in self.data_centers.iter() {
            let other = data_center.read().await.max_volume_id;
            if other > vid {
                vid = other;
            }
        }
        vid
    }
}

impl Topology {
    pub async fn is_leader(&self) -> bool {
        match self.raft.as_ref() {
            Some(raft) => raft.is_leader().await.is_ok(),
            None => false,
        }
    }

    pub async fn leader(&self) -> Option<NodeId> {
        match self.raft.as_ref() {
            Some(raft) => raft.current_leader().await,
            None => None,
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum TopologyError {
    #[error("Other error: {0}")]
    Box(#[from] Box<dyn std::error::Error + Sync + Send>),
}

pub async fn topology_vacuum_loop(
    topology: TopologyRef,
    garbage_threshold: f64,
    preallocate: u64,
    mut shutdown: async_broadcast::Receiver<()>,
) {
    info!("topology vacuum loop starting");
    let mut interval = tokio::time::interval(Duration::from_secs(15 * 60));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                debug!("topology vacuum starting.");
                topology.write().await.vacuum(garbage_threshold, preallocate).await;
                debug!("topology vacuum success.")
            }
            _ = shutdown.recv() => {
                break;
            }
        }
    }
    info!("topology vacuum loop stopped")
}

#[derive(Clone)]
pub struct TopologyRef(Arc<RwLock<Topology>>);

impl TopologyRef {
    pub fn new(sequencer: Sequencer, volume_size_limit: u64, pulse: u64) -> Self {
        Self(Arc::new(RwLock::new(Topology::new(
            sequencer,
            volume_size_limit,
            pulse,
        ))))
    }

    pub async fn read(&self) -> tokio::sync::RwLockReadGuard<'_, Topology> {
        self.0.read().await
    }

    pub async fn write(&self) -> tokio::sync::RwLockWriteGuard<'_, Topology> {
        self.0.write().await
    }

    pub fn downgrade(&self) -> WeakTopologyRef {
        WeakTopologyRef(Arc::downgrade(&self.0))
    }
}

#[derive(Clone)]
pub struct WeakTopologyRef(Weak<RwLock<Topology>>);

impl WeakTopologyRef {
    pub fn new() -> Self {
        Self(Weak::new())
    }

    pub fn upgrade(&self) -> Option<TopologyRef> {
        self.0.upgrade().map(TopologyRef)
    }
}

impl Default for WeakTopologyRef {
    fn default() -> Self {
        Self::new()
    }
}
