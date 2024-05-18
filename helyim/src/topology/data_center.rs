use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Weak},
};

use dashmap::DashMap;
use faststr::FastStr;
use serde::Serialize;
use tokio::sync::RwLock;

use crate::{
    storage::{VolumeError, VolumeId},
    topology::{
        node::NodeImpl,
        rack::{Rack, RackRef},
        DataNodeRef, Topology,
    },
};

#[derive(Serialize)]
pub struct DataCenter {
    node: NodeImpl,
    // parent
    #[serde(skip)]
    pub topology: RwLock<Weak<Topology>>,
    // children
    #[serde(skip)]
    pub racks: DashMap<FastStr, RackRef>,
}

impl DataCenter {
    pub fn new(id: FastStr) -> DataCenter {
        let node = NodeImpl::new(id);
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

    pub async fn reserve_one_volume(&self, mut random: i64) -> Result<DataNodeRef, VolumeError> {
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
        if !self.racks.contains_key(rack.id()) {
            self.adjust_max_volume_count(rack.max_volume_count()).await;
            self.adjust_max_volume_id(rack.max_volume_id()).await;
            self.adjust_volume_count(rack.volume_count()).await;
            self.adjust_ec_shard_count(rack.ec_shard_count()).await;
            self.adjust_active_volume_count(rack._active_volume_count())
                .await;

            self.racks.insert(rack.id.clone(), rack);
        }
    }

    pub fn volume_count(&self) -> i64 {
        let mut count = 0;
        for rack in self.racks.iter() {
            count += rack.volume_count();
        }
        count
    }

    pub fn max_volume_count(&self) -> i64 {
        let mut max_volumes = 0;
        for rack in self.racks.iter() {
            max_volumes += rack.max_volume_count();
        }
        max_volumes
    }

    pub fn free_space(&self) -> i64 {
        let mut free_space = 0;
        for rack in self.racks.iter() {
            free_space += rack.free_space();
        }
        free_space
    }

    pub async fn adjust_volume_count(&self, volume_count_delta: i64) {
        self._adjust_volume_count(volume_count_delta);

        if let Some(topo) = self.topology.read().await.upgrade() {
            topo.adjust_volume_count(volume_count_delta);
        }
    }

    pub async fn adjust_active_volume_count(&self, active_volume_count_delta: i64) {
        self._adjust_active_volume_count(active_volume_count_delta);

        if let Some(topo) = self.topology.read().await.upgrade() {
            topo.adjust_active_volume_count(active_volume_count_delta);
        }
    }

    pub async fn adjust_ec_shard_count(&self, ec_shard_count_delta: i64) {
        self._adjust_ec_shard_count(ec_shard_count_delta);

        if let Some(topo) = self.topology.read().await.upgrade() {
            topo.adjust_ec_shard_count(ec_shard_count_delta);
        }
    }

    pub async fn adjust_max_volume_count(&self, max_volume_count_delta: i64) {
        self._adjust_max_volume_count(max_volume_count_delta);

        if let Some(topo) = self.topology.read().await.upgrade() {
            topo.adjust_max_volume_count(max_volume_count_delta);
        }
    }

    pub async fn adjust_max_volume_id(&self, vid: VolumeId) {
        self._adjust_max_volume_id(vid);

        if let Some(topo) = self.topology.read().await.upgrade() {
            topo.adjust_max_volume_id(self.max_volume_id());
        }
    }
}

impl Deref for DataCenter {
    type Target = NodeImpl;

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
