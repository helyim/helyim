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
        data_center::DataCenter,
        data_node::DataNode,
        node::{Node, NodeImpl, NodeType},
        DataNodeRef,
    },
};

#[derive(Serialize)]
pub struct Rack {
    node: Arc<NodeImpl>,
    // children
    #[serde(skip)]
    pub data_nodes: DashMap<FastStr, DataNodeRef>,
    // parent
    #[serde(skip)]
    pub data_center: RwLock<Weak<DataCenter>>,
}

impl Rack {
    pub fn new(id: FastStr) -> Rack {
        let node = Arc::new(NodeImpl::new(id));
        Self {
            node,
            data_nodes: DashMap::new(),
            data_center: RwLock::new(Weak::new()),
        }
    }

    pub async fn set_data_center(&self, data_center: Weak<DataCenter>) {
        *self.data_center.write().await = data_center;
    }

    pub async fn get_or_create_data_node(
        &self,
        id: FastStr,
        ip: FastStr,
        port: u16,
        public_url: FastStr,
        max_volume_count: i64,
    ) -> DataNodeRef {
        match self.data_nodes.get(&id) {
            Some(data_node) => data_node.value().clone(),
            None => {
                let data_node = Arc::new(DataNode::new(
                    id.clone(),
                    ip,
                    port,
                    public_url,
                    max_volume_count,
                ));
                self.link_data_node(data_node.clone()).await;
                data_node
            }
        }
    }

    pub async fn data_center_id(&self) -> FastStr {
        match self.node.parent().await {
            Some(data_center) => FastStr::new(data_center.id()),
            None => FastStr::empty(),
        }
    }

    pub async fn reserve_one_volume(&self, mut random: i64) -> StdResult<DataNodeRef, VolumeError> {
        for data_node in self.data_nodes.iter() {
            let free_space = data_node.free_space();
            if free_space <= 0 {
                continue;
            }
            if random >= free_space {
                random -= free_space;
            } else {
                return Ok(data_node.clone());
            }
        }

        Err(VolumeError::NoFreeSpace(format!(
            "no free volumes found on rack {}",
            self.id()
        )))
    }
}

impl Rack {
    pub async fn link_data_node(&self, data_node: Arc<DataNode>) {
        let rack_node = self.node.clone();
        rack_node.link_child_node(data_node).await;
    }
}

#[async_trait::async_trait]
impl Node for Rack {
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
        NodeType::Rack
    }

    fn children(&self) -> Vec<Arc<dyn Node>> {
        self.node.children()
    }

    async fn parent(&self) -> Option<Arc<dyn Node>> {
        self.node.parent().await
    }
}

pub type RackRef = Arc<Rack>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use faststr::FastStr;
    use rand::Rng;

    use crate::topology::rack::Rack;

    #[tokio::test]
    pub async fn test_get_or_create_data_node() {
        let rack = Rack::new(FastStr::new("default"));

        let id = FastStr::new("127.0.0.1:8080");
        let node = rack
            .get_or_create_data_node(id.clone(), FastStr::new("127.0.0.1"), 8080, id.clone(), 1)
            .await;

        let _node1 = rack
            .get_or_create_data_node(id.clone(), FastStr::new("127.0.0.1"), 8080, id, 1)
            .await;

        assert_eq!(Arc::strong_count(&node), 3);
    }

    #[tokio::test]
    pub async fn test_reserve_one_volume() {
        let rack = Rack::new(FastStr::new("default"));

        let id = FastStr::new("127.0.0.1:8080");
        let node = rack
            .get_or_create_data_node(id.clone(), FastStr::new("127.0.0.1"), 8080, id.clone(), 1)
            .await;

        let random = rand::thread_rng().gen_range(0..rack.free_space());
        let _node1 = rack.reserve_one_volume(random).await.unwrap();

        assert_eq!(Arc::strong_count(&node), 3);
    }
}
