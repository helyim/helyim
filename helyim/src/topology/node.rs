use std::{
    any::Any,
    fmt::{Display, Formatter},
    sync::{
        atomic::{AtomicI64, AtomicU32, Ordering},
        Arc, Weak,
    },
};

use dashmap::DashMap;
use faststr::FastStr;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::info;

use crate::{
    errors::Error,
    storage::{erasure_coding::DATA_SHARDS_COUNT, VolumeError, VolumeId},
    topology::{data_node::DataNode, DataNodeRef},
};

pub type NodeId = FastStr;

#[derive(Serialize, Deserialize, Clone)]
pub struct NodeImpl {
    pub id: NodeId,
    volume_count: Arc<AtomicI64>,
    active_volume_count: Arc<AtomicI64>,
    ec_shard_count: Arc<AtomicI64>,
    max_volume_count: Arc<AtomicI64>,
    max_volume_id: Arc<AtomicU32>,

    parent: RwLock<Weak<dyn Node>>,
    children: DashMap<NodeId, Arc<dyn Node>>,
}

impl NodeImpl {
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            volume_count: Arc::new(AtomicI64::new(0)),
            active_volume_count: Arc::new(AtomicI64::new(0)),
            ec_shard_count: Arc::new(AtomicI64::new(0)),
            max_volume_count: Arc::new(AtomicI64::new(0)),
            max_volume_id: Arc::new(AtomicU32::new(0)),
            parent: RwLock::new(Weak::new()),
            children: DashMap::new(),
        }
    }

    pub fn set_max_volume_count(&self, max_volume_count: i64) {
        self.max_volume_count
            .store(max_volume_count, Ordering::Relaxed);
    }
}

pub enum NodeType {
    None,
    DataNode,
    Rack,
    DataCenter,
}

impl NodeType {
    pub fn is_data_node(&self) -> bool {
        matches!(self, NodeType::DataNode)
    }

    pub fn is_data_center(&self) -> bool {
        matches!(self, NodeType::DataCenter)
    }

    pub fn is_rack(&self) -> bool {
        matches!(self, NodeType::Rack)
    }
}

impl Display for NodeType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeType::DataNode => write!(f, "data_node"),
            NodeType::Rack => write!(f, "rack"),
            NodeType::DataCenter => write!(f, "data_center"),
            NodeType::None => write!(f, "none"),
        }
    }
}

#[async_trait::async_trait]
pub trait Node: Any + Send + Sync {
    fn id(&self) -> &str;
    fn free_space(&self) -> i64;
    fn reserve_one_volume(&self, rand: i64) -> Result<DataNodeRef, VolumeError>;
    async fn adjust_max_volume_count(&self, max_volume_count_delta: i64);
    async fn adjust_volume_count(&self, volume_count_delta: i64);
    async fn adjust_ec_shard_count(&self, ec_shard_count_delta: i64);
    async fn adjust_active_volume_count(&self, active_volume_count_delta: i64);
    async fn adjust_max_volume_id(&self, vid: VolumeId);

    fn volume_count(&self) -> i64;
    fn ec_shard_count(&self) -> i64;
    fn active_volume_count(&self) -> i64;
    fn max_volume_count(&self) -> i64;
    fn max_volume_id(&self) -> VolumeId;

    async fn set_parent(&self, parent: Weak<dyn Node>);
    async fn link_child_node(self: Arc<Self>, child: Arc<dyn Node>);
    async fn unlink_child_node(&self, node_id: NodeId);

    fn node_type(&self) -> NodeType {
        NodeType::None
    }

    fn children(&self) -> Vec<Arc<dyn Node>>;
    async fn parent(&self) -> Option<Arc<dyn Node>> {
        None
    }
}

impl Node for NodeImpl {
    fn id(&self) -> &str {
        &self.id
    }

    fn free_space(&self) -> i64 {
        let mut free_volume_slot_count = self.max_volume_count() + self.volume_count();
        if self.ec_shard_count() > 0 {
            free_volume_slot_count =
                free_volume_slot_count - self.ec_shard_count() / DATA_SHARDS_COUNT as i64 - 1;
        }
        free_volume_slot_count
    }

    fn reserve_one_volume(&self, mut rand: i64) -> Result<DataNodeRef, VolumeError> {
        for child in self.children.iter() {
            let free_space = child.free_space();
            if free_space <= 0 {
                continue;
            }
            if rand >= free_space {
                rand -= free_space;
            } else {
                if child.node_type().is_data_node() && child.free_space() > 0 {
                    let child = child.value().clone();
                    let child = child.downcast::<DataNode>().expect("expect DataNode type");
                    return Ok(child);
                }
                return child.reserve_one_volume(rand);
            }
        }

        Err(VolumeError::NoFreeSpace(format!(
            "no free volumes found {}, node type: {}",
            self.id(),
            self.node_type()
        )))
    }

    async fn adjust_max_volume_count(&self, max_volume_count_delta: i64) {
        self.max_volume_count
            .fetch_add(max_volume_count_delta, Ordering::Relaxed);
    }

    async fn adjust_volume_count(&self, volume_count_delta: i64) {
        self.volume_count
            .fetch_add(volume_count_delta, Ordering::Relaxed);
    }

    async fn adjust_ec_shard_count(&self, ec_shard_count_delta: i64) {
        self.ec_shard_count
            .fetch_add(ec_shard_count_delta, Ordering::Relaxed);
    }

    async fn adjust_active_volume_count(&self, active_volume_count_delta: i64) {
        self.active_volume_count
            .fetch_add(active_volume_count_delta, Ordering::Relaxed);
    }

    async fn adjust_max_volume_id(&self, vid: VolumeId) {
        if self.max_volume_id() < vid {
            self.max_volume_id.store(vid, Ordering::Relaxed);
        }
    }

    fn volume_count(&self) -> i64 {
        self.volume_count.load(Ordering::Relaxed)
    }

    fn ec_shard_count(&self) -> i64 {
        self.ec_shard_count.load(Ordering::Relaxed)
    }

    fn active_volume_count(&self) -> i64 {
        self.active_volume_count.load(Ordering::Relaxed)
    }

    fn max_volume_count(&self) -> i64 {
        self.max_volume_count.load(Ordering::Relaxed)
    }

    fn max_volume_id(&self) -> VolumeId {
        self.max_volume_id.load(Ordering::Relaxed)
    }

    async fn set_parent(&self, parent: Weak<dyn Node>) {
        *self.parent.write().await = parent;
    }

    async fn link_child_node(self: Arc<Self>, child: Arc<dyn Node>) {
        if !self.children.contains_key(child.id()) {
            self.adjust_max_volume_count(child.max_volume_count()).await;
            self.adjust_max_volume_id(child.max_volume_id()).await;
            self.adjust_volume_count(child.volume_count()).await;
            self.adjust_ec_shard_count(child.ec_shard_count()).await;
            self.adjust_active_volume_count(child.active_volume_count())
                .await;

            child.set_parent(Arc::downgrade(&self)).await;

            let node_id = FastStr::new(child.id());
            info!("add child {node_id}");
            self.children.insert(node_id, child);
        }
    }

    async fn unlink_child_node(&self, node_id: NodeId) {
        if let Some((node_id, node)) = self.children.remove(&node_id) {
            node.set_parent(Weak::new()).await;

            self.adjust_max_volume_count(-node.max_volume_count()).await;
            self.adjust_volume_count(-node.volume_count()).await;
            self.adjust_ec_shard_count(-node.ec_shard_count()).await;
            self.adjust_active_volume_count(-node.active_volume_count())
                .await;

            info!("remove child {node_id}");
        }
    }

    fn children(&self) -> Vec<Arc<dyn Node>> {
        let mut childs = Vec::with_capacity(self.children.len());
        for child in self.children.iter() {
            childs.push(child.clone());
        }
        childs
    }

    async fn parent(&self) -> Option<Arc<dyn Node>> {
        self.parent.read().await.upgrade()
    }
}
