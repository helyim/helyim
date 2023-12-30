use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Weak},
};

use dashmap::DashMap;
use faststr::FastStr;
use rand::Rng;
use serde::Serialize;
use tokio::sync::RwLock;

use crate::{
    storage::{VolumeError, VolumeId},
    topology::{
        node::Node,
        rack::{Rack, RackRef},
        DataNodeRef, Topology,
    },
};

#[derive(Serialize)]
pub struct DataCenter {
    node: Node,
    // parent
    #[serde(skip)]
    pub topology: RwLock<Weak<Topology>>,
    // children
    #[serde(skip)]
    pub racks: DashMap<FastStr, RackRef>,
}

impl DataCenter {
    pub fn new(id: FastStr) -> DataCenter {
        let node = Node::new(id);
        Self {
            node,
            topology: RwLock::new(Weak::new()),
            racks: DashMap::new(),
        }
    }

    pub async fn set_topology(&self, topology: Weak<Topology>) {
        *self.topology.write().await = topology;
    }

    pub fn get_or_create_rack(&self, id: FastStr) -> RackRef {
        match self.racks.get(&id) {
            Some(rack) => rack.value().clone(),
            None => {
                let rack = Arc::new(Rack::new(id.clone()));
                self.racks.insert(id, rack.clone());
                rack
            }
        }
    }

    pub async fn reserve_one_volume(&self) -> Result<DataNodeRef, VolumeError> {
        // randomly select one
        let mut free_volumes = 0;
        for rack in self.racks.iter() {
            free_volumes += rack.free_volumes().await;
        }

        let idx = rand::thread_rng().gen_range(0..free_volumes);

        for rack in self.racks.iter() {
            free_volumes -= rack.free_volumes().await;
            if free_volumes == idx {
                return rack.reserve_one_volume().await;
            }
        }

        Err(VolumeError::NoFreeSpace(format!(
            "no free volumes found on data center {}",
            self.id()
        )))
    }
}

impl DataCenter {
    pub async fn volume_count(&self) -> u64 {
        let mut count = 0;
        for rack in self.racks.iter() {
            count += rack.volume_count().await;
        }
        count
    }

    pub async fn max_volume_count(&self) -> u64 {
        let mut max_volumes = 0;
        for rack in self.racks.iter() {
            max_volumes += rack.max_volume_count().await;
        }
        max_volumes
    }

    pub async fn free_volumes(&self) -> u64 {
        let mut free_volumes = 0;
        for rack in self.racks.iter() {
            free_volumes += rack.free_volumes().await;
        }
        free_volumes
    }

    pub async fn adjust_volume_count(&self, volume_count_delta: i64) {
        self._adjust_volume_count(volume_count_delta);

        if let Some(topo) = self.topology.read().await.upgrade() {
            topo.adjust_volume_count(volume_count_delta).await;
        }
    }

    pub async fn adjust_active_volume_count(&self, active_volume_count_delta: i64) {
        self._adjust_active_volume_count(active_volume_count_delta);

        if let Some(topo) = self.topology.read().await.upgrade() {
            topo.adjust_active_volume_count(active_volume_count_delta)
                .await;
        }
    }

    pub async fn adjust_ec_shard_count(&self, ec_shard_count_delta: i64) {
        self._adjust_ec_shard_count(ec_shard_count_delta);

        if let Some(topo) = self.topology.read().await.upgrade() {
            topo.adjust_ec_shard_count(ec_shard_count_delta).await;
        }
    }

    pub async fn adjust_max_volume_count(&self, max_volume_count_delta: i64) {
        self._adjust_max_volume_count(max_volume_count_delta);

        if let Some(topo) = self.topology.read().await.upgrade() {
            topo.adjust_max_volume_count(max_volume_count_delta).await;
        }
    }

    pub async fn adjust_max_volume_id(&self, vid: VolumeId) {
        self._adjust_max_volume_id(vid);

        if let Some(topo) = self.topology.read().await.upgrade() {
            topo.adjust_max_volume_id(self.max_volume_id()).await;
        }
    }
}

impl Deref for DataCenter {
    type Target = Node;

    fn deref(&self) -> &Self::Target {
        &self.node
    }
}

impl DerefMut for DataCenter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.node
    }
}

pub type DataCenterRef = Arc<DataCenter>;
