use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    result::Result as StdResult,
    sync::{Arc, Weak},
};

use faststr::FastStr;
use rand::Rng;
use serde::Serialize;
use tokio::sync::RwLock;

use crate::{
    errors::Result,
    storage::{VolumeError, VolumeId},
    topology::{data_center::WeakDataCenterRef, node::Node, DataNodeRef},
};

#[derive(Serialize)]
pub struct Rack {
    node: Node,
    // children
    #[serde(skip)]
    pub data_nodes: HashMap<FastStr, DataNodeRef>,
    // parent
    #[serde(skip)]
    pub data_center: WeakDataCenterRef,
}

impl Rack {
    pub fn new(id: FastStr) -> Rack {
        let node = Node::new(id);
        Self {
            node,
            data_nodes: HashMap::new(),
            data_center: WeakDataCenterRef::new(),
        }
    }

    pub fn set_data_center(&mut self, data_center: WeakDataCenterRef) {
        self.data_center = data_center;
    }

    pub async fn get_or_create_data_node(
        &mut self,
        id: FastStr,
        ip: FastStr,
        port: u16,
        public_url: FastStr,
        max_volume_count: u64,
    ) -> Result<DataNodeRef> {
        match self.data_nodes.get(&id) {
            Some(data_node) => Ok(data_node.clone()),
            None => {
                let data_node =
                    DataNodeRef::new(id.clone(), ip, port, public_url, max_volume_count).await?;

                self.data_nodes.insert(id, data_node.clone());
                Ok(data_node)
            }
        }
    }

    pub async fn data_center_id(&self) -> FastStr {
        match self.data_center.upgrade() {
            Some(data_center) => data_center.read().await.id.clone(),
            None => FastStr::empty(),
        }
    }

    pub async fn reserve_one_volume(&self) -> StdResult<DataNodeRef, VolumeError> {
        // randomly select
        let mut free_volumes = 0;
        for (_, data_node) in self.data_nodes.iter() {
            free_volumes += data_node.read().await.free_volumes();
        }

        let idx = rand::thread_rng().gen_range(0..free_volumes);

        for (_, data_node) in self.data_nodes.iter() {
            free_volumes -= data_node.read().await.free_volumes();
            if free_volumes == idx {
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
    pub async fn volume_count(&self) -> u64 {
        let mut count = 0;
        for data_node in self.data_nodes.values() {
            count += data_node.read().await.volume_count();
        }
        count
    }

    pub async fn max_volume_count(&self) -> u64 {
        let mut max_volumes = 0;
        for data_node in self.data_nodes.values() {
            max_volumes += data_node.read().await.max_volume_count();
        }
        max_volumes
    }

    pub async fn free_volumes(&self) -> u64 {
        let mut free_volumes = 0;
        for data_node in self.data_nodes.values() {
            free_volumes += data_node.read().await.free_volumes();
        }
        free_volumes
    }

    pub async fn adjust_volume_count(&self, volume_count_delta: i64) {
        self._adjust_volume_count(volume_count_delta);

        if let Some(dc) = self.data_center.upgrade() {
            dc.read()
                .await
                .adjust_volume_count(volume_count_delta)
                .await;
        }
    }

    pub async fn adjust_active_volume_count(&self, active_volume_count_delta: i64) {
        self._adjust_active_volume_count(active_volume_count_delta);

        if let Some(dc) = self.data_center.upgrade() {
            dc.read()
                .await
                .adjust_active_volume_count(active_volume_count_delta)
                .await;
        }
    }

    pub async fn adjust_ec_shard_count(&self, ec_shard_count_delta: i64) {
        self._adjust_ec_shard_count(ec_shard_count_delta);

        if let Some(dc) = self.data_center.upgrade() {
            dc.read()
                .await
                .adjust_ec_shard_count(ec_shard_count_delta)
                .await;
        }
    }

    pub async fn adjust_max_volume_count(&self, max_volume_count_delta: i64) {
        self._adjust_max_volume_count(max_volume_count_delta);

        if let Some(dc) = self.data_center.upgrade() {
            dc.read()
                .await
                .adjust_max_volume_count(max_volume_count_delta)
                .await;
        }
    }

    pub async fn adjust_max_volume_id(&self, vid: VolumeId) {
        self._adjust_max_volume_id(vid);

        if let Some(dc) = self.data_center.upgrade() {
            dc.read()
                .await
                .adjust_max_volume_id(self.max_volume_id())
                .await;
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

#[derive(Clone)]
pub struct RackRef(Arc<RwLock<Rack>>);

impl RackRef {
    pub fn new(id: FastStr) -> Self {
        Self(Arc::new(RwLock::new(Rack::new(id))))
    }

    pub async fn read(&self) -> tokio::sync::RwLockReadGuard<'_, Rack> {
        self.0.read().await
    }

    pub async fn write(&self) -> tokio::sync::RwLockWriteGuard<'_, Rack> {
        self.0.write().await
    }

    pub fn downgrade(&self) -> WeakRackRef {
        WeakRackRef(Arc::downgrade(&self.0))
    }
}

#[derive(Clone)]
pub struct WeakRackRef(Weak<RwLock<Rack>>);

impl Default for WeakRackRef {
    fn default() -> Self {
        Self::new()
    }
}

impl WeakRackRef {
    pub fn new() -> Self {
        Self(Weak::new())
    }

    pub fn upgrade(&self) -> Option<RackRef> {
        self.0.upgrade().map(RackRef)
    }
}
