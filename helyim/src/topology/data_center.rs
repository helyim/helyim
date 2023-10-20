use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use async_lock::RwLock;
use dashmap::DashMap;
use faststr::FastStr;
use rand::random;
use serde::Serialize;

use crate::{
    errors::{Error, Result},
    storage::VolumeId,
    topology::{DataNode, Rack},
};

#[derive(Debug, Serialize)]
pub struct DataCenter {
    pub id: FastStr,
    max_volume_id: AtomicU32,
    #[serde(skip)]
    pub racks: Arc<DashMap<FastStr, Arc<RwLock<Rack>>>>,
}

impl DataCenter {
    pub fn new(id: FastStr) -> DataCenter {
        DataCenter {
            id,
            racks: Arc::new(DashMap::new()),
            max_volume_id: AtomicU32::new(0),
        }
    }

    pub fn max_volume_id(&self) -> VolumeId {
        self.max_volume_id.load(Ordering::Relaxed)
    }

    pub fn adjust_max_volume_id(&self, vid: VolumeId) {
        if vid > self.max_volume_id.load(Ordering::Relaxed) {
            self.max_volume_id.store(vid, Ordering::Relaxed);
        }
    }

    pub fn get_or_create_rack(&self, id: FastStr) -> Arc<RwLock<Rack>> {
        self.racks
            .entry(id.clone())
            .or_insert_with(|| Arc::new(RwLock::new(Rack::new(id))))
            .clone()
    }

    pub async fn has_volumes(&self) -> Result<i64> {
        let mut ret = 0;
        for rack in self.racks.iter() {
            ret += rack.read().await.has_volumes().await?;
        }
        Ok(ret)
    }

    pub async fn max_volumes(&self) -> Result<i64> {
        let mut ret = 0;
        for rack in self.racks.iter() {
            ret += rack.read().await.max_volumes().await?;
        }
        Ok(ret)
    }

    pub async fn free_volumes(&self) -> Result<i64> {
        let mut free_volumes = 0;
        for rack in self.racks.iter() {
            free_volumes += rack.read().await.free_volumes().await?;
        }
        Ok(free_volumes)
    }

    pub async fn reserve_one_volume(&self) -> Result<Arc<RwLock<DataNode>>> {
        // randomly select one
        let mut free_volumes = 0;
        for rack in self.racks.iter() {
            free_volumes += rack.read().await.free_volumes().await?;
        }

        let idx = random::<u32>() as i64 % free_volumes;

        for rack in self.racks.iter() {
            free_volumes -= rack.read().await.free_volumes().await?;
            if free_volumes == idx {
                return rack.read().await.reserve_one_volume().await;
            }
        }

        Err(Error::NoFreeSpace(format!("data center: {}", self.id)))
    }
}
