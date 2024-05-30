use std::{
    fmt::{Display, Formatter},
    sync::{
        atomic::{AtomicI64, AtomicU32, Ordering},
        Arc,
    },
};

use dashmap::DashMap;
use downcast_rs::{impl_downcast, DowncastSync};
use faststr::FastStr;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::info;

use crate::{
    storage::{erasure_coding::DATA_SHARDS_COUNT, VolumeError, VolumeId},
    topology::{
        data_center::{DataCenter, DataCenterRef},
        data_node::DataNode,
        rack::{Rack, RackRef},
        DataNodeRef,
    },
};

pub type NodeId = FastStr;

#[derive(Serialize, Deserialize)]
pub struct NodeImpl {
    pub id: NodeId,
    volume_count: AtomicI64,
    active_volume_count: AtomicI64,
    ec_shard_count: AtomicI64,
    max_volume_count: AtomicI64,
    max_volume_id: AtomicU32,

    #[serde(skip)]
    parent: RwLock<Option<Arc<dyn Node>>>,
    #[serde(skip)]
    children: DashMap<NodeId, Arc<dyn Node>>,
}

impl NodeImpl {
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            volume_count: AtomicI64::new(0),
            active_volume_count: AtomicI64::new(0),
            ec_shard_count: AtomicI64::new(0),
            max_volume_count: AtomicI64::new(0),
            max_volume_id: AtomicU32::new(0),
            parent: RwLock::new(None),
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
    Topology,
    DataNode,
    Rack,
    DataCenter,
}

impl NodeType {
    pub fn is_data_node(&self) -> bool {
        matches!(self, NodeType::DataNode)
    }
}

impl Display for NodeType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeType::DataNode => write!(f, "data_node"),
            NodeType::Rack => write!(f, "rack"),
            NodeType::DataCenter => write!(f, "data_center"),
            NodeType::Topology => write!(f, "topology"),
            NodeType::None => write!(f, "none"),
        }
    }
}

#[async_trait::async_trait]
pub trait Node: DowncastSync {
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

    async fn set_parent(&self, parent: Option<Arc<dyn Node>>);
    async fn link_child_node(self: Arc<Self>, child: Arc<dyn Node>);
    async fn unlink_child_node(&self, node_id: &str);

    fn node_type(&self) -> NodeType {
        NodeType::None
    }

    fn children(&self) -> &DashMap<NodeId, Arc<dyn Node>>;
    async fn parent(&self) -> Option<Arc<dyn Node>> {
        None
    }
}

impl_downcast!(sync Node);

#[async_trait::async_trait]
impl Node for NodeImpl {
    fn id(&self) -> &str {
        &self.id
    }

    fn free_space(&self) -> i64 {
        let mut free_volume_slot_count = self.max_volume_count() - self.volume_count();
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
                    return downcast_node(child.clone());
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
        if let Some(parent) = self.parent.read().await.as_ref() {
            parent.adjust_max_volume_count(max_volume_count_delta).await;
        }
    }

    async fn adjust_volume_count(&self, volume_count_delta: i64) {
        self.volume_count
            .fetch_add(volume_count_delta, Ordering::Relaxed);
        if let Some(parent) = self.parent.read().await.as_ref() {
            parent.adjust_volume_count(volume_count_delta).await;
        }
    }

    async fn adjust_ec_shard_count(&self, ec_shard_count_delta: i64) {
        self.ec_shard_count
            .fetch_add(ec_shard_count_delta, Ordering::Relaxed);
        if let Some(parent) = self.parent.read().await.as_ref() {
            parent.adjust_ec_shard_count(ec_shard_count_delta).await;
        }
    }

    async fn adjust_active_volume_count(&self, active_volume_count_delta: i64) {
        self.active_volume_count
            .fetch_add(active_volume_count_delta, Ordering::Relaxed);
        if let Some(parent) = self.parent.read().await.as_ref() {
            parent
                .adjust_active_volume_count(active_volume_count_delta)
                .await;
        }
    }

    async fn adjust_max_volume_id(&self, vid: VolumeId) {
        if self.max_volume_id() < vid {
            self.max_volume_id.store(vid, Ordering::Relaxed);
            if let Some(parent) = self.parent.read().await.as_ref() {
                parent.adjust_max_volume_id(vid).await;
            }
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

    async fn set_parent(&self, parent: Option<Arc<dyn Node>>) {
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

            child.set_parent(Some(self.clone())).await;

            let node_id = FastStr::new(child.id());
            info!("add child {node_id}, node type: {}", child.node_type());
            self.children.insert(node_id, child);
        }
    }

    async fn unlink_child_node(&self, node_id: &str) {
        if let Some((node_id, node)) = self.children.remove(node_id) {
            node.set_parent(None).await;

            self.adjust_max_volume_count(-node.max_volume_count()).await;
            self.adjust_volume_count(-node.volume_count()).await;
            self.adjust_ec_shard_count(-node.ec_shard_count()).await;
            self.adjust_active_volume_count(-node.active_volume_count())
                .await;

            info!("remove child {node_id}");
        }
    }

    fn children(&self) -> &DashMap<NodeId, Arc<dyn Node>> {
        &self.children
    }

    async fn parent(&self) -> Option<Arc<dyn Node>> {
        self.parent.read().await.clone()
    }
}

macro_rules! impl_node {
    ($node:ident) => {
        #[async_trait::async_trait]
        impl Node for $node {
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
                NodeType::$node
            }

            fn children(&self) -> &DashMap<faststr::FastStr, Arc<dyn Node>> {
                self.node.children()
            }

            async fn parent(&self) -> Option<Arc<dyn Node>> {
                self.node.parent().await
            }
        }
    };
}

pub fn downcast_node(node: Arc<dyn Node>) -> Result<DataNodeRef, VolumeError> {
    node.downcast_arc::<DataNode>()
        .map_err(|_| VolumeError::WrongNodeType)
}

pub fn downcast_rack(node: Arc<dyn Node>) -> Result<RackRef, VolumeError> {
    node.downcast_arc::<Rack>()
        .map_err(|_| VolumeError::WrongNodeType)
}

pub fn downcast_data_center(node: Arc<dyn Node>) -> Result<DataCenterRef, VolumeError> {
    node.downcast_arc::<DataCenter>()
        .map_err(|_| VolumeError::WrongNodeType)
}

#[cfg(test)]
mod tests {
    use crate::topology::{node::Node, tests::setup_topo};

    #[tokio::test]
    pub async fn test_node() {
        let topo = setup_topo().await;
        assert_eq!(topo.active_volume_count(), 15);
        assert_eq!(topo.volume_count(), 15);
        assert_eq!(topo.max_volume_count(), 30);
        assert_eq!(topo.ec_shard_count(), 0);

        for child in topo.children().iter() {
            let dc = child.key().as_str();

            match dc {
                "dc1" => {
                    assert_eq!(child.volume_count(), 12);
                    assert_eq!(child.max_volume_count(), 26);
                    assert_eq!(child.active_volume_count(), 12);
                    assert_eq!(child.ec_shard_count(), 0);
                }
                "dc2" => {
                    assert_eq!(child.volume_count(), 0);
                    assert_eq!(child.max_volume_count(), 0);
                    assert_eq!(child.active_volume_count(), 0);
                    assert_eq!(child.ec_shard_count(), 0);
                }
                "dc3" => {
                    assert_eq!(child.volume_count(), 3);
                    assert_eq!(child.max_volume_count(), 4);
                    assert_eq!(child.active_volume_count(), 3);
                    assert_eq!(child.ec_shard_count(), 0);
                }
                _ => {
                    panic!("should not be here.");
                }
            }

            for child in child.children().iter() {
                let rack = child.key().as_str();

                match rack {
                    "rack11" => {
                        assert_eq!(child.volume_count(), 6);
                        assert_eq!(child.max_volume_count(), 13);
                        assert_eq!(child.active_volume_count(), 6);
                        assert_eq!(child.ec_shard_count(), 0);
                    }
                    "rack12" => {
                        assert_eq!(child.volume_count(), 6);
                        assert_eq!(child.max_volume_count(), 13);
                        assert_eq!(child.active_volume_count(), 6);
                        assert_eq!(child.ec_shard_count(), 0);
                    }
                    "rack32" => {
                        assert_eq!(child.volume_count(), 3);
                        assert_eq!(child.max_volume_count(), 4);
                        assert_eq!(child.active_volume_count(), 3);
                        assert_eq!(child.ec_shard_count(), 0);
                    }
                    _ => {
                        panic!("should not be here.");
                    }
                }

                for child in child.children().iter() {
                    let data_node = child.key().as_str();

                    match data_node {
                        "server111" => {
                            assert_eq!(child.volume_count(), 3);
                            assert_eq!(child.max_volume_count(), 3);
                            assert_eq!(child.active_volume_count(), 3);
                            assert_eq!(child.ec_shard_count(), 0);

                            child.adjust_volume_count(-1).await;
                        }
                        "server112" => {
                            assert_eq!(child.volume_count(), 3);
                            assert_eq!(child.max_volume_count(), 10);
                            assert_eq!(child.active_volume_count(), 3);
                            assert_eq!(child.ec_shard_count(), 0);

                            child.adjust_active_volume_count(-1).await;
                        }
                        "server121" => {
                            assert_eq!(child.volume_count(), 3);
                            assert_eq!(child.max_volume_count(), 4);
                            assert_eq!(child.active_volume_count(), 3);
                            assert_eq!(child.ec_shard_count(), 0);

                            child.adjust_ec_shard_count(1).await;
                        }
                        "server122" => {
                            assert_eq!(child.volume_count(), 0);
                            assert_eq!(child.max_volume_count(), 4);
                            assert_eq!(child.active_volume_count(), 0);
                            assert_eq!(child.ec_shard_count(), 0);

                            child.adjust_max_volume_count(4).await;
                        }
                        "server123" => {
                            assert_eq!(child.volume_count(), 3);
                            assert_eq!(child.max_volume_count(), 5);
                            assert_eq!(child.active_volume_count(), 3);
                            assert_eq!(child.ec_shard_count(), 0);
                        }
                        "server321" => {
                            assert_eq!(child.volume_count(), 3);
                            assert_eq!(child.max_volume_count(), 4);
                            assert_eq!(child.active_volume_count(), 3);
                            assert_eq!(child.ec_shard_count(), 0);
                        }
                        _ => {
                            panic!("should not be here.");
                        }
                    }
                }

                match rack {
                    "rack11" => {
                        assert_eq!(child.volume_count(), 5);
                        assert_eq!(child.max_volume_count(), 13);
                        assert_eq!(child.active_volume_count(), 5);
                        assert_eq!(child.ec_shard_count(), 0);
                    }
                    "rack12" => {
                        assert_eq!(child.volume_count(), 6);
                        assert_eq!(child.max_volume_count(), 17);
                        assert_eq!(child.active_volume_count(), 6);
                        assert_eq!(child.ec_shard_count(), 1);
                    }
                    "rack32" => {
                        assert_eq!(child.volume_count(), 3);
                        assert_eq!(child.max_volume_count(), 4);
                        assert_eq!(child.active_volume_count(), 3);
                        assert_eq!(child.ec_shard_count(), 0);
                    }
                    _ => {
                        panic!("should not be here.");
                    }
                }
            }

            match dc {
                "dc1" => {
                    assert_eq!(child.volume_count(), 11);
                    assert_eq!(child.max_volume_count(), 30);
                    assert_eq!(child.active_volume_count(), 11);
                    assert_eq!(child.ec_shard_count(), 1);
                }
                "dc2" => {
                    assert_eq!(child.volume_count(), 0);
                    assert_eq!(child.max_volume_count(), 0);
                    assert_eq!(child.active_volume_count(), 0);
                    assert_eq!(child.ec_shard_count(), 0);
                }
                "dc3" => {
                    assert_eq!(child.volume_count(), 3);
                    assert_eq!(child.max_volume_count(), 4);
                    assert_eq!(child.active_volume_count(), 3);
                    assert_eq!(child.ec_shard_count(), 0);
                }
                _ => {
                    panic!("should not be here.");
                }
            }
        }

        assert_eq!(topo.active_volume_count(), 14);
        assert_eq!(topo.volume_count(), 14);
        assert_eq!(topo.max_volume_count(), 34);
        assert_eq!(topo.ec_shard_count(), 1);
    }
}
