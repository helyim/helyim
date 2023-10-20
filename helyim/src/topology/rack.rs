use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc, Weak,
};

use async_lock::RwLock;
use dashmap::DashMap;
use faststr::FastStr;
use rand::random;
use serde::Serialize;

use crate::{
    errors::{Error, Result},
    storage::VolumeId,
    topology::{DataCenter, DataNode},
};

#[derive(Debug, Serialize)]
pub struct Rack {
    pub id: FastStr,
    #[serde(skip)]
    nodes: Arc<DashMap<FastStr, Arc<RwLock<DataNode>>>>,
    max_volume_id: AtomicU32,
    #[serde(skip)]
    pub data_center: Weak<DataCenter>,
}

impl Rack {
    pub fn new(id: FastStr) -> Rack {
        Rack {
            id,
            nodes: Arc::new(DashMap::new()),
            max_volume_id: AtomicU32::new(0),
            data_center: Weak::new(),
        }
    }

    pub fn set_data_center(&mut self, data_center: Weak<DataCenter>) {
        self.data_center = data_center;
    }
    pub fn data_nodes(&self) -> Arc<DashMap<FastStr, Arc<RwLock<DataNode>>>> {
        self.nodes.clone()
    }

    pub fn adjust_max_volume_id(&self, vid: VolumeId) -> Result<()> {
        if vid > self.max_volume_id.load(Ordering::Relaxed) {
            self.max_volume_id.store(vid, Ordering::Relaxed);
        }

        if let Some(dc) = self.data_center.upgrade() {
            dc.adjust_max_volume_id(self.max_volume_id.load(Ordering::Relaxed));
        }

        Ok(())
    }

    pub fn get_or_create_data_node(
        &self,
        id: FastStr,
        ip: FastStr,
        port: u16,
        public_url: FastStr,
        max_volumes: i64,
    ) -> Arc<RwLock<DataNode>> {
        self.nodes
            .entry(id.clone())
            .or_insert_with(|| {
                Arc::new(RwLock::new(DataNode::new(
                    id,
                    ip,
                    port,
                    public_url,
                    max_volumes,
                )))
            })
            .clone()
    }

    pub fn data_center_id(&self) -> FastStr {
        match self.data_center.upgrade() {
            Some(data_center) => data_center.id.clone(),
            None => FastStr::empty(),
        }
    }

    pub async fn has_volumes(&self) -> Result<i64> {
        let mut count = 0;
        for dn_tx in self.nodes.iter() {
            count += dn_tx.read().await.has_volumes();
        }
        Ok(count)
    }

    pub async fn max_volumes(&self) -> Result<i64> {
        let mut max_volumes = 0;
        for dn_tx in self.nodes.iter() {
            max_volumes += dn_tx.read().await.max_volumes();
        }
        Ok(max_volumes)
    }

    pub async fn free_volumes(&self) -> Result<i64> {
        let mut free_volumes = 0;
        for dn_tx in self.nodes.iter() {
            free_volumes += dn_tx.read().await.free_volumes();
        }
        Ok(free_volumes)
    }

    pub async fn reserve_one_volume(&self) -> Result<Arc<RwLock<DataNode>>> {
        // randomly select
        let mut free_volumes = 0;
        for dn_tx in self.nodes.iter() {
            free_volumes += dn_tx.read().await.free_volumes();
        }

        let idx = random::<u32>() as i64 % free_volumes;

        for dn_tx in self.nodes.iter() {
            free_volumes -= dn_tx.read().await.free_volumes();
            if free_volumes == idx {
                return Ok(dn_tx.clone());
            }
        }

        Err(Error::NoFreeSpace(format!(
            "reserve one volume on rack {} fail",
            self.id
        )))
    }
}
