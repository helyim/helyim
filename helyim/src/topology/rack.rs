use std::{
    ops::{Deref, DerefMut},
    result::Result as StdResult,
    sync::{Arc, Weak},
};

use dashmap::DashMap;
use faststr::FastStr;
use serde::Serialize;
use tokio::sync::RwLock;

use crate::{
    storage::{VolumeError, VolumeId},
    topology::{data_center::DataCenter, data_node::DataNode, node::Node, DataNodeRef},
};

#[derive(Serialize)]
pub struct Rack {
    node: Node,
    // children
    #[serde(skip)]
    pub data_nodes: DashMap<FastStr, DataNodeRef>,
    // parent
    #[serde(skip)]
    pub data_center: RwLock<Weak<DataCenter>>,
}

impl Rack {
    pub fn new(id: FastStr) -> Rack {
        let node = Node::new(id);
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
        match self.data_center.read().await.upgrade() {
            Some(data_center) => data_center.id.clone(),
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
        if !self.data_nodes.contains_key(data_node.id()) {
            self.adjust_max_volume_count(data_node.max_volume_count())
                .await;
            self.adjust_max_volume_id(data_node.max_volume_id()).await;
            self.adjust_volume_count(data_node.volume_count()).await;
            self.adjust_ec_shard_count(data_node.ec_shard_count()).await;
            self.adjust_active_volume_count(data_node.active_volume_count())
                .await;

            self.data_nodes.insert(data_node.id.clone(), data_node);
        }
    }

    pub async fn unlink_data_node(&self, id: &str) {
        if let Some((_, data_node)) = self.data_nodes.remove(id) {
            self.adjust_max_volume_count(-data_node.max_volume_count())
                .await;
            self.adjust_volume_count(-data_node.volume_count()).await;
            self.adjust_ec_shard_count(-data_node.ec_shard_count())
                .await;
            self.adjust_active_volume_count(-data_node.active_volume_count())
                .await;
            *data_node.rack.write().await = Weak::new();
        }
    }

    pub fn volume_count(&self) -> i64 {
        let mut count = 0;
        for data_node in self.data_nodes.iter() {
            count += data_node.volume_count();
        }
        count
    }

    pub fn max_volume_count(&self) -> i64 {
        let mut max_volumes = 0;
        for data_node in self.data_nodes.iter() {
            max_volumes += data_node.max_volume_count();
        }
        max_volumes
    }

    pub fn free_space(&self) -> i64 {
        let mut free_space = 0;
        for data_node in self.data_nodes.iter() {
            free_space += data_node.free_space();
        }
        free_space
    }

    pub async fn adjust_volume_count(&self, volume_count_delta: i64) {
        self._adjust_volume_count(volume_count_delta);

        if let Some(dc) = self.data_center.read().await.upgrade() {
            dc.adjust_volume_count(volume_count_delta).await;
        }
    }

    pub async fn adjust_active_volume_count(&self, active_volume_count_delta: i64) {
        self._adjust_active_volume_count(active_volume_count_delta);

        if let Some(dc) = self.data_center.read().await.upgrade() {
            dc.adjust_active_volume_count(active_volume_count_delta)
                .await;
        }
    }

    pub async fn adjust_ec_shard_count(&self, ec_shard_count_delta: i64) {
        self._adjust_ec_shard_count(ec_shard_count_delta);

        if let Some(dc) = self.data_center.read().await.upgrade() {
            dc.adjust_ec_shard_count(ec_shard_count_delta).await;
        }
    }

    pub async fn adjust_max_volume_count(&self, max_volume_count_delta: i64) {
        self._adjust_max_volume_count(max_volume_count_delta);

        if let Some(dc) = self.data_center.read().await.upgrade() {
            dc.adjust_max_volume_count(max_volume_count_delta).await;
        }
    }

    pub async fn adjust_max_volume_id(&self, vid: VolumeId) {
        self._adjust_max_volume_id(vid);

        if let Some(dc) = self.data_center.read().await.upgrade() {
            dc.adjust_max_volume_id(self.max_volume_id()).await;
        }
    }
}

impl Deref for Rack {
    type Target = Node;

    fn deref(&self) -> &Self::Target {
        &self.node
    }
}

impl DerefMut for Rack {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.node
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
