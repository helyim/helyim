use std::{
    collections::BTreeMap,
    ops::{Deref, DerefMut},
    result::Result as StdResult,
    sync::Arc,
    time::Duration,
};

use axum::{
    http::{
        header::{InvalidHeaderName, InvalidHeaderValue},
        StatusCode,
    },
    response::{IntoResponse, Response},
    Json,
};
use dashmap::DashMap;
use faststr::FastStr;
use helyim_proto::directory::{
    VolumeInformationMessage, VolumeLocation, VolumeShortInformationMessage,
};
use serde::Serialize;
use serde_json::json;
use tokio::sync::{mpsc::UnboundedSender, RwLock};
use tonic::Status;
use tracing::{debug, error, info};

use crate::{
    raft::{types::NodeId, RaftServer},
    rt_spawn,
    sequence::{Sequence, Sequencer},
    storage::{
        batch_vacuum_volume_check, batch_vacuum_volume_commit, batch_vacuum_volume_compact, FileId,
        ReplicaPlacement, Ttl, VolumeError, VolumeId, VolumeInfo,
    },
    topology::{
        collection::Collection,
        data_center::{DataCenter, DataCenterRef},
        erasure_coding::EcShardLocations,
        node::Node,
        volume_grow::VolumeGrowOption,
        volume_layout::VolumeLayoutRef,
        DataNodeRef,
    },
};

#[derive(Serialize)]
pub struct Topology {
    node: Node,
    #[serde(skip)]
    sequencer: Sequencer,
    pub collections: DashMap<FastStr, Collection>,
    #[serde(skip)]
    pub ec_shards: DashMap<VolumeId, EcShardLocations>,
    pulse: u64,
    volume_size_limit: u64,
    // children
    #[serde(skip)]
    pub(crate) data_centers: DashMap<FastStr, DataCenterRef>,

    #[serde(skip)]
    raft: RwLock<Option<RaftServer>>,
}

impl Clone for Topology {
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
            sequencer: self.sequencer.clone(),
            collections: self.collections.clone(),
            ec_shards: self.ec_shards.clone(),
            pulse: self.pulse,
            volume_size_limit: self.volume_size_limit,
            data_centers: DashMap::new(),
            raft: RwLock::new(None),
        }
    }
}

impl Topology {
    pub fn new(sequencer: Sequencer, volume_size_limit: u64, pulse: u64) -> Topology {
        let node = Node::new(FastStr::new("topo"));
        Topology {
            node,
            sequencer,
            collections: DashMap::new(),
            ec_shards: DashMap::new(),
            pulse,
            volume_size_limit,
            data_centers: DashMap::new(),
            raft: RwLock::new(None),
        }
    }

    pub async fn get_or_create_data_center(&self, name: &str) -> DataCenterRef {
        match self.data_centers.get(name) {
            Some(data_node) => data_node.value().clone(),
            None => {
                let data_center = Arc::new(DataCenter::new(FastStr::new(name)));
                self.link_data_center(data_center.clone());
                data_center
            }
        }
    }

    pub async fn lookup(&self, collection: &str, volume_id: VolumeId) -> Option<Vec<DataNodeRef>> {
        if collection.is_empty() {
            for c in self.collections.iter() {
                let data_node = c.lookup(volume_id).await;
                if data_node.is_some() {
                    return data_node;
                }
            }
        } else if let Some(c) = self.collections.get(collection) {
            let data_node = c.lookup(volume_id).await;
            if data_node.is_some() {
                return data_node;
            }
        }

        None
    }

    pub async fn has_writable_volume(&self, option: &VolumeGrowOption) -> bool {
        let vl = self.get_volume_layout(
            option.collection.clone(),
            option.replica_placement,
            option.ttl,
        );

        let active_volume_count = vl.active_volume_count(option).await;
        active_volume_count > 0
    }

    pub async fn pick_for_write(
        &self,
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
            let (vid, nodes) = layout.pick_for_write(option.as_ref()).await?;
            (vid, nodes[0].clone())
        };

        let file_id = FileId::new(volume_id, file_id, rand::random::<u32>());
        Ok((file_id, count, node))
    }

    pub async fn register_data_node(
        &self,
        dc_name: &str,
        rack_name: &str,
        data_node: &DataNodeRef,
    ) {
        if data_node.rack.read().await.upgrade().is_some() {
            return;
        }
        let dc = self.get_or_create_data_center(dc_name).await;
        let rack = dc.get_or_create_rack(rack_name).await;
        rack.link_data_node(data_node.clone()).await;
        info!("data node: {} relink to topology", data_node.id());
    }

    pub async fn sync_data_node_registration(
        &self,
        volumes: &[VolumeInformationMessage],
        data_node: &DataNodeRef,
    ) -> (Vec<VolumeInfo>, Vec<VolumeInfo>) {
        let mut volume_infos = Vec::new();
        for volume in volumes {
            match VolumeInfo::new(volume) {
                Ok(volume) => volume_infos.push(volume),
                Err(err) => error!("failed to convert joined volume information: {err}"),
            }
        }

        let (new_volumes, deleted_volumes) = data_node.update_volumes(volume_infos).await;
        for volume in new_volumes.iter() {
            self.register_volume_layout(volume, data_node).await;
        }
        for volume in deleted_volumes.iter() {
            self.unregister_volume_layout(volume, data_node).await;
        }

        (new_volumes, deleted_volumes)
    }

    pub async fn incremental_sync_data_node_registration(
        &self,
        new_volumes: &[VolumeShortInformationMessage],
        deleted_volumes: &[VolumeShortInformationMessage],
        data_node: &DataNodeRef,
    ) {
        let mut new_vis = vec![];
        let mut old_vis = vec![];

        for volume in new_volumes {
            match VolumeInfo::new_from_short(volume) {
                Ok(volume) => new_vis.push(volume),
                Err(err) => {
                    info!("create short volume info error: {err}");
                    continue;
                }
            }
        }
        for volume in deleted_volumes {
            match VolumeInfo::new_from_short(volume) {
                Ok(volume) => old_vis.push(volume),
                Err(err) => {
                    info!("create short volume info error: {err}");
                    continue;
                }
            }
        }

        data_node.delta_update_volumes(&new_vis, &old_vis).await;

        for v in new_vis.iter() {
            self.register_volume_layout(v, data_node).await;
        }
        for v in old_vis.iter() {
            self.unregister_volume_layout(v, data_node).await;
        }
    }

    pub async fn register_volume_layout(&self, volume: &VolumeInfo, data_node: &DataNodeRef) {
        self.get_volume_layout(
            volume.collection.clone(),
            volume.replica_placement,
            volume.ttl,
        )
        .register_volume(volume, data_node)
        .await
    }

    pub async fn unregister_volume_layout(&self, volume: &VolumeInfo, data_node: &DataNodeRef) {
        self.get_volume_layout(
            volume.collection.clone(),
            volume.replica_placement,
            volume.ttl,
        )
        .unregister_volume(volume, data_node)
        .await;
    }

    pub async fn unregister_data_node(&self, data_node: &DataNodeRef) {
        for volume in data_node.volumes.iter() {
            self.get_volume_layout(
                volume.collection.clone(),
                volume.replica_placement,
                volume.ttl,
            )
            .set_volume_unavailable(volume.key(), data_node)
            .await;
        }

        data_node
            .adjust_volume_count(-data_node.volume_count())
            .await;
        data_node
            .adjust_active_volume_count(-data_node.active_volume_count())
            .await;
        data_node
            .adjust_max_volume_count(-data_node.max_volume_count())
            .await;
        if let Some(rack) = data_node.rack.read().await.upgrade() {
            rack.unlink_data_node(data_node.id()).await;
        }
    }

    pub async fn next_volume_id(&self) -> Result<VolumeId, VolumeError> {
        let vid = self.max_volume_id();
        let next = vid + 1;
        if let Some(raft) = self.raft.read().await.as_ref() {
            let raft = raft.clone();
            rt_spawn(async move {
                if let Err(err) = raft.set_max_volume_id(next).await {
                    error!("set max volume id failed, error: {err}");
                }
            });
        }
        Ok(next)
    }

    pub fn set_max_sequence(&self, seq: u64) {
        self.sequencer.set_max(seq);
    }

    pub fn topology(&self) -> Topology {
        self.clone()
    }

    pub async fn vacuum(&self, garbage_threshold: f64, preallocate: u64) {
        for collection in self.collections.iter() {
            for volume_layout in collection.volume_layouts.iter() {
                let volume_layout = volume_layout.value().clone();
                // TODO: avoid cloning the HashMap
                let locations = volume_layout.locations.clone();
                for data_nodes in locations.iter() {
                    let vid = *data_nodes.key();
                    if volume_layout.readonly_volumes.contains_key(&vid) {
                        continue;
                    }

                    if batch_vacuum_volume_check(vid, &data_nodes, garbage_threshold).await
                        && batch_vacuum_volume_compact(
                            &volume_layout,
                            vid,
                            &data_nodes,
                            preallocate,
                        )
                        .await
                    {
                        batch_vacuum_volume_commit(&volume_layout, vid, &data_nodes).await;
                        // let _ = batch_vacuum_volume_cleanup(vid, data_nodes).await;
                    }
                }
            }
        }
    }
}

impl Topology {
    fn get_volume_layout(
        &self,
        collection_name: FastStr,
        rp: ReplicaPlacement,
        ttl: Ttl,
    ) -> VolumeLayoutRef {
        match self.collections.get(&collection_name) {
            Some(collection) => collection.get_or_create_volume_layout(rp, Some(ttl)),
            None => {
                let collection = Collection::new(collection_name.clone(), self.volume_size_limit);
                let vl = collection.get_or_create_volume_layout(rp, Some(ttl));
                self.collections.insert(collection_name, collection);
                vl
            }
        }
    }
}

impl Topology {
    pub async fn set_raft_server(&self, raft: RaftServer) {
        *self.raft.write().await = Some(raft);
    }

    pub async fn current_leader_id(&self) -> Option<NodeId> {
        match self.raft.read().await.as_ref() {
            Some(raft) => raft.current_leader().await,
            None => None,
        }
    }

    pub async fn current_leader_address(&self) -> Option<FastStr> {
        match self.raft.read().await.as_ref() {
            Some(raft) => raft.current_leader_address().await,
            None => None,
        }
    }

    pub async fn current_leader(&self) -> Result<FastStr, TopologyError> {
        match self.raft.read().await.as_ref() {
            Some(raft) => match raft.current_leader_address().await {
                Some(leader) => Ok(leader),
                None => Err(TopologyError::NoLeader),
            },
            None => Err(TopologyError::NoLeader),
        }
    }

    pub async fn is_leader(&self) -> bool {
        match self.raft.read().await.as_ref() {
            Some(raft) => raft.is_leader().await,
            None => false,
        }
    }

    pub async fn peers(&self) -> BTreeMap<NodeId, FastStr> {
        match self.raft.read().await.as_ref() {
            Some(raft) => raft.peers(),
            None => BTreeMap::new(),
        }
    }

    pub async fn inform_new_leader(&self, tx: &UnboundedSender<Result<VolumeLocation, Status>>) {
        if let Ok(leader) = self.current_leader().await {
            let mut location = VolumeLocation::new();
            location.leader = Some(leader.to_string());

            let _ = tx.send(Ok(location));
        }
    }
}

impl Topology {
    pub fn to_volume_locations(&self) -> Vec<VolumeLocation> {
        let mut locations = Vec::new();

        for data_center in self.data_centers.iter() {
            for rack in data_center.racks.iter() {
                for data_node in rack.data_nodes.iter() {
                    let mut location = VolumeLocation::new();
                    location.url = data_node.url();
                    location.public_url = data_node.public_url.to_string();

                    for volume in data_node.volumes.iter() {
                        location.new_vids.push(volume.id);
                    }
                    for volume in data_node.ec_shards.iter() {
                        location.new_vids.push(volume.volume_id);
                    }
                    locations.push(location);
                }
            }
        }
        locations
    }

    fn link_data_center(&self, data_center: Arc<DataCenter>) {
        if !self.data_centers.contains_key(data_center.id()) {
            self.adjust_max_volume_count(data_center.max_volume_count());
            self.adjust_max_volume_id(data_center.max_volume_id());
            self.adjust_volume_count(data_center.volume_count());
            self.adjust_ec_shard_count(data_center.ec_shard_count());
            self.adjust_active_volume_count(data_center.active_volume_count());

            self.data_centers
                .insert(data_center.id.clone(), data_center);
        }
    }

    pub fn volume_count(&self) -> i64 {
        let mut count = 0;
        for dc in self.data_centers.iter() {
            count += dc.volume_count();
        }
        count
    }

    pub fn max_volume_count(&self) -> i64 {
        let mut max_volumes = 0;
        for dc in self.data_centers.iter() {
            max_volumes += dc.max_volume_count();
        }
        max_volumes
    }

    pub fn free_space(&self) -> i64 {
        let mut free_space = 0;
        for dc in self.data_centers.iter() {
            free_space += dc.free_space();
        }
        free_space
    }

    pub fn adjust_volume_count(&self, volume_count_delta: i64) {
        self._adjust_volume_count(volume_count_delta);
    }

    pub fn adjust_active_volume_count(&self, active_volume_count_delta: i64) {
        self._adjust_active_volume_count(active_volume_count_delta);
    }

    pub fn adjust_ec_shard_count(&self, ec_shard_count_delta: i64) {
        self._adjust_ec_shard_count(ec_shard_count_delta);
    }

    pub fn adjust_max_volume_count(&self, max_volume_count_delta: i64) {
        self._adjust_max_volume_count(max_volume_count_delta);
    }

    pub fn adjust_max_volume_id(&self, vid: VolumeId) {
        self._adjust_max_volume_id(vid);
    }
}

impl Deref for Topology {
    type Target = Node;

    fn deref(&self) -> &Self::Target {
        &self.node
    }
}

impl DerefMut for Topology {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.node
    }
}

#[derive(thiserror::Error, Debug)]
pub enum TopologyError {
    #[error("{0}")]
    Box(#[from] Box<dyn std::error::Error + Sync + Send>),

    #[error("This raft cluster has no leader")]
    NoLeader,

    #[error("Hyper error: {0}")]
    Hyper(#[from] hyper::Error),
    #[error("Invalid header value: {0}")]
    InvalidHeaderValue(#[from] InvalidHeaderValue),
    #[error("Invalid header name: {0}")]
    InvalidHeaderName(#[from] InvalidHeaderName),
    #[error("Invalid uri: {0}")]
    InvalidUrl(#[from] hyper::http::uri::InvalidUri),
}

impl IntoResponse for TopologyError {
    fn into_response(self) -> Response {
        let error = self.to_string();
        let error = json!({
            "error": error
        });
        let response = (StatusCode::BAD_REQUEST, Json(error));
        response.into_response()
    }
}

pub async fn topology_vacuum_loop(
    topology: TopologyRef,
    garbage_threshold: f64,
    preallocate: u64,
    mut shutdown: async_broadcast::Receiver<()>,
) {
    info!("topology vacuum loop starting");
    let mut interval = tokio::time::interval(Duration::from_secs(15));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                if topology.is_leader().await {
                    debug!("topology vacuum starting.");
                    topology.vacuum(garbage_threshold, preallocate).await;
                    debug!("topology vacuum success.")
                }
            }
            _ = shutdown.recv() => {
                break;
            }
        }
    }
    info!("topology vacuum loop stopped")
}

pub type TopologyRef = Arc<Topology>;
