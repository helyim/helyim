use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    sync::{Arc, Weak},
};

use faststr::FastStr;
use rand::Rng;
use serde::Serialize;

use crate::{
    storage::{VolumeError, VolumeId},
    topology::{node::Node, rack::Rack, topology::SimpleTopology, DataNodeRef},
};

#[derive(Serialize)]
pub struct DataCenter {
    inner: Arc<SimpleDataCenter>,
    // children
    #[serde(skip)]
    pub racks: HashMap<FastStr, Rack>,
}

impl DataCenter {
    pub fn new(id: FastStr) -> DataCenter {
        Self {
            inner: Arc::new(SimpleDataCenter::new(id)),
            racks: HashMap::new(),
        }
    }

    pub fn get_or_create_rack(&mut self, id: FastStr) -> &mut Rack {
        self.racks
            .entry(id.clone())
            .or_insert_with(|| Rack::new(id))
    }

    pub async fn reserve_one_volume(&self) -> Result<DataNodeRef, VolumeError> {
        // randomly select one
        let mut free_volumes = 0;
        for (_, rack) in self.racks.iter() {
            free_volumes += rack.free_volumes().await;
        }

        let idx = rand::thread_rng().gen_range(0..free_volumes);

        for (_, rack) in self.racks.iter() {
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
    pub fn downgrade(&self) -> Weak<SimpleDataCenter> {
        Arc::downgrade(&self.inner)
    }

    pub async fn volume_count(&self) -> u64 {
        let mut count = 0;
        for rack in self.racks.values() {
            count += rack.volume_count().await;
        }
        count
    }

    pub async fn max_volume_count(&self) -> u64 {
        let mut max_volumes = 0;
        for rack in self.racks.values() {
            max_volumes += rack.max_volume_count().await;
        }
        max_volumes
    }

    pub async fn free_volumes(&self) -> u64 {
        let mut free_volumes = 0;
        for rack in self.racks.values() {
            free_volumes += rack.free_volumes().await;
        }
        free_volumes
    }
}

#[derive(Serialize)]
pub struct SimpleDataCenter {
    node: Node,
    // parent
    #[serde(skip)]
    pub topology: Weak<SimpleTopology>,
}

impl SimpleDataCenter {
    pub fn new(id: FastStr) -> Self {
        Self {
            node: Node::new(id),
            topology: Weak::new(),
        }
    }

    pub fn set_topology(&self, topology: Weak<SimpleTopology>) {
        unsafe {
            let this = self as *const Self as *mut Self;
            (*this).topology = topology;
        }
    }

    pub async fn adjust_volume_count(&self, volume_count_delta: i64) {
        self._adjust_volume_count(volume_count_delta);

        if let Some(topo) = self.topology.upgrade() {
            topo.adjust_volume_count(volume_count_delta).await;
        }
    }

    pub async fn adjust_active_volume_count(&self, active_volume_count_delta: i64) {
        self._adjust_active_volume_count(active_volume_count_delta);

        if let Some(topo) = self.topology.upgrade() {
            topo.adjust_active_volume_count(active_volume_count_delta)
                .await;
        }
    }

    pub async fn adjust_ec_shard_count(&self, ec_shard_count_delta: i64) {
        self._adjust_ec_shard_count(ec_shard_count_delta);

        if let Some(topo) = self.topology.upgrade() {
            topo.adjust_ec_shard_count(ec_shard_count_delta).await;
        }
    }

    pub async fn adjust_max_volume_count(&self, max_volume_count_delta: i64) {
        self._adjust_max_volume_count(max_volume_count_delta);

        if let Some(topo) = self.topology.upgrade() {
            topo.adjust_max_volume_count(max_volume_count_delta).await;
        }
    }

    pub async fn adjust_max_volume_id(&self, vid: VolumeId) {
        self._adjust_max_volume_id(vid);

        if let Some(topo) = self.topology.upgrade() {
            topo.adjust_max_volume_id(self.max_volume_id()).await;
        }
    }
}

impl Deref for DataCenter {
    type Target = Arc<SimpleDataCenter>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for DataCenter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Deref for SimpleDataCenter {
    type Target = Node;

    fn deref(&self) -> &Self::Target {
        &self.node
    }
}

impl DerefMut for SimpleDataCenter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.node
    }
}
