use std::{
    result::Result as StdResult,
    sync::{Arc, Weak},
};

use dashmap::DashMap;
use faststr::FastStr;
use serde::Serialize;
use tokio::sync::RwLock;

use crate::{
    storage::{VolumeError, VolumeId},
    topology::{
        node::{Node, NodeImpl, NodeType},
        rack::{Rack, RackRef},
        DataNodeRef, Topology,
    },
};

#[derive(Serialize)]
pub struct DataCenter {
    node: Arc<NodeImpl>,
    // parent
    #[serde(skip)]
    pub topology: RwLock<Weak<Topology>>,
    // children
    #[serde(skip)]
    pub racks: DashMap<FastStr, RackRef>,
}

impl DataCenter {
    pub fn new(id: FastStr) -> DataCenter {
        let node = Arc::new(NodeImpl::new(id));
        Self {
            node,
            topology: RwLock::new(Weak::new()),
            racks: DashMap::new(),
        }
    }

    pub async fn set_topology(&self, topology: Weak<Topology>) {
        *self.topology.write().await = topology;
    }

    pub async fn get_or_create_rack(&self, id: &str) -> RackRef {
        match self.racks.get(id) {
            Some(rack) => rack.value().clone(),
            None => {
                let rack = Arc::new(Rack::new(FastStr::new(id)));
                self.link_rack(rack.clone()).await;
                rack
            }
        }
    }

    pub async fn reserve_one_volume(&self, mut random: i64) -> StdResult<DataNodeRef, VolumeError> {
        for rack in self.racks.iter() {
            let free_space = rack.free_space();
            if free_space <= 0 {
                continue;
            }
            if random >= free_space {
                random -= free_space;
            } else {
                let node = rack.reserve_one_volume(random).await?;
                return Ok(node);
            }
        }

        Err(VolumeError::NoFreeSpace(format!(
            "no free volumes found on data center {}",
            self.id()
        )))
    }
}

impl DataCenter {
    pub async fn link_rack(&self, rack: Arc<Rack>) {
        let data_center_node = self.node.clone();
        data_center_node.link_child_node(rack).await;
    }
}

#[async_trait::async_trait]
impl Node for DataCenter {
    fn id(&self) -> &str {
        self.node.id()
    }

    /// number of free volume slots
    fn free_space(&self) -> i64 {
        self.node.free_space()
    }

    fn reserve_one_volume(&self, rand: i64) -> StdResult<DataNodeRef, VolumeError> {
        self.node.reserve_one_volume(rand)
    }

    async fn adjust_max_volume_count(&self, max_volume_count_delta: i64) {
        self.node
            .adjust_max_volume_count(max_volume_count_delta)
            .await;
    }

    async fn adjust_volume_count(&self, volume_count_delta: i64) {
        self.node.adjust_volume_count(volume_count_delta).await;
    }

    async fn adjust_ec_shard_count(&self, ec_shard_count_delta: i64) {
        self.node.adjust_ec_shard_count(ec_shard_count_delta).await;
    }

    async fn adjust_active_volume_count(&self, active_volume_count_delta: i64) {
        self.node
            .adjust_active_volume_count(active_volume_count_delta)
            .await;
    }

    async fn adjust_max_volume_id(&self, vid: VolumeId) {
        self.node.adjust_max_volume_id(vid).await;
    }

    fn volume_count(&self) -> i64 {
        self.node.volume_count()
    }

    fn ec_shard_count(&self) -> i64 {
        self.node.ec_shard_count()
    }

    fn active_volume_count(&self) -> i64 {
        self.node.active_volume_count()
    }

    fn max_volume_count(&self) -> i64 {
        self.node.max_volume_count()
    }

    fn max_volume_id(&self) -> VolumeId {
        self.node.max_volume_id()
    }

    async fn set_parent(&self, parent: Option<Arc<dyn Node>>) {
        self.node.set_parent(parent).await
    }

    async fn link_child_node(self: Arc<Self>, _child: Arc<dyn Node>) {}

    async fn unlink_child_node(&self, node_id: &str) {
        self.node.unlink_child_node(node_id).await;
    }

    fn node_type(&self) -> NodeType {
        NodeType::DataCenter
    }

    fn children(&self) -> Vec<Arc<dyn Node>> {
        self.node.children()
    }

    async fn parent(&self) -> Option<Arc<dyn Node>> {
        self.node.parent().await
    }
}

pub type DataCenterRef = Arc<DataCenter>;
