use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    result::Result as StdResult,
    sync::{Arc, Weak},
};

use faststr::FastStr;
use rand::Rng;
use serde::Serialize;

use crate::{
    errors::Result,
    storage::{VolumeError, VolumeId},
    topology::{data_center::SimpleDataCenter, node::Node, DataNodeRef},
};

#[derive(Serialize)]
pub struct Rack {
    // children
    #[serde(skip)]
    pub data_nodes: HashMap<FastStr, DataNodeRef>,
    pub inner: Arc<SimpleRack>,
}

impl Rack {
    pub fn new(id: FastStr) -> Rack {
        Self {
            inner: Arc::new(SimpleRack::new(id)),
            data_nodes: HashMap::new(),
        }
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
    pub fn downgrade(&self) -> Weak<SimpleRack> {
        Arc::downgrade(&self.inner)
    }

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
}

#[derive(Serialize)]
pub struct SimpleRack {
    node: Node,
    // parent
    data_center: Weak<SimpleDataCenter>,
}

impl SimpleRack {
    pub fn new(id: FastStr) -> Self {
        Self {
            node: Node::new(id),
            data_center: Weak::new(),
        }
    }

    pub fn set_data_center(&self, data_center: Weak<SimpleDataCenter>) {
        unsafe {
            let this = self as *const Self as *mut Self;
            (*this).data_center = data_center;
        }
    }

    pub async fn data_center_id(&self) -> FastStr {
        match self.data_center.upgrade() {
            Some(data_center) => data_center.id.clone(),
            None => FastStr::empty(),
        }
    }

    pub async fn adjust_volume_count(&self, volume_count_delta: i64) {
        self._adjust_volume_count(volume_count_delta);

        if let Some(dc) = self.data_center.upgrade() {
            dc.adjust_volume_count(volume_count_delta).await;
        }
    }

    pub async fn adjust_active_volume_count(&self, active_volume_count_delta: i64) {
        self._adjust_active_volume_count(active_volume_count_delta);

        if let Some(dc) = self.data_center.upgrade() {
            dc.adjust_active_volume_count(active_volume_count_delta)
                .await;
        }
    }

    pub async fn adjust_ec_shard_count(&self, ec_shard_count_delta: i64) {
        self._adjust_ec_shard_count(ec_shard_count_delta);

        if let Some(dc) = self.data_center.upgrade() {
            dc.adjust_ec_shard_count(ec_shard_count_delta).await;
        }
    }

    pub async fn adjust_max_volume_count(&self, max_volume_count_delta: i64) {
        self._adjust_max_volume_count(max_volume_count_delta);

        if let Some(dc) = self.data_center.upgrade() {
            dc.adjust_max_volume_count(max_volume_count_delta).await;
        }
    }

    pub async fn adjust_max_volume_id(&self, vid: VolumeId) {
        self._adjust_max_volume_id(vid);

        if let Some(dc) = self.data_center.upgrade() {
            dc.adjust_max_volume_id(self.max_volume_id()).await;
        }
    }
}

impl Deref for Rack {
    type Target = Arc<SimpleRack>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Rack {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Deref for SimpleRack {
    type Target = Node;

    fn deref(&self) -> &Self::Target {
        &self.node
    }
}

impl DerefMut for SimpleRack {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.node
    }
}
