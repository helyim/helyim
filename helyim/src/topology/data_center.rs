use std::{
    collections::HashMap,
    sync::{Arc, Weak},
};

use faststr::FastStr;
use rand::random;
use serde::Serialize;
use tokio::sync::RwLock;

use crate::{
    errors::{Error, Result},
    storage::VolumeId,
    topology::{rack::RackRef, DataNodeRef},
};

#[derive(Serialize)]
pub struct DataCenter {
    pub id: FastStr,
    pub max_volume_id: VolumeId,
    // children
    #[serde(skip)]
    pub racks: HashMap<FastStr, RackRef>,
}

impl DataCenter {
    pub fn new(id: FastStr) -> DataCenter {
        Self {
            id: id.clone(),
            racks: HashMap::new(),
            max_volume_id: 0,
        }
    }

    pub fn adjust_max_volume_id(&mut self, vid: VolumeId) {
        if vid > self.max_volume_id {
            self.max_volume_id = vid;
        }
    }

    pub fn get_or_create_rack(&mut self, id: FastStr) -> RackRef {
        match self.racks.get(&id) {
            Some(rack) => rack.clone(),
            None => {
                let rack = RackRef::new(id.clone());
                self.racks.insert(id, rack.clone());
                rack
            }
        }
    }

    pub async fn has_volumes(&self) -> Result<i64> {
        let mut has_volumes = 0;
        for rack in self.racks.values() {
            has_volumes += rack.read().await.has_volumes().await?;
        }
        Ok(has_volumes)
    }

    pub async fn max_volumes(&self) -> Result<i64> {
        let mut max_volumes = 0;
        for rack in self.racks.values() {
            max_volumes += rack.read().await.max_volumes().await;
        }
        Ok(max_volumes)
    }

    pub async fn free_volumes(&self) -> Result<i64> {
        let mut free_volumes = 0;
        for rack in self.racks.values() {
            free_volumes += rack.read().await.free_volumes().await?;
        }
        Ok(free_volumes)
    }

    pub async fn reserve_one_volume(&self) -> Result<DataNodeRef> {
        // randomly select one
        let mut free_volumes = 0;
        for (_, rack) in self.racks.iter() {
            free_volumes += rack.read().await.free_volumes().await?;
        }

        let idx = random::<u32>() as i64 % free_volumes;

        for (_, rack) in self.racks.iter() {
            free_volumes -= rack.read().await.free_volumes().await?;
            if free_volumes == idx {
                return rack.read().await.reserve_one_volume().await;
            }
        }

        Err(Error::NoFreeSpace(format!(
            "no free volumes found on data center {}",
            self.id
        )))
    }
}

#[derive(Clone)]
pub struct DataCenterRef(Arc<RwLock<DataCenter>>);

impl DataCenterRef {
    pub fn new(id: FastStr) -> Self {
        Self(Arc::new(RwLock::new(DataCenter::new(id))))
    }

    pub async fn read(&self) -> tokio::sync::RwLockReadGuard<'_, DataCenter> {
        self.0.read().await
    }

    pub async fn write(&self) -> tokio::sync::RwLockWriteGuard<'_, DataCenter> {
        self.0.write().await
    }

    pub fn downgrade(&self) -> WeakDataCenterRef {
        WeakDataCenterRef(Arc::downgrade(&self.0))
    }
}

#[derive(Clone)]
pub struct WeakDataCenterRef(Weak<RwLock<DataCenter>>);

impl Default for WeakDataCenterRef {
    fn default() -> Self {
        Self::new()
    }
}

impl WeakDataCenterRef {
    pub fn new() -> Self {
        Self(Weak::new())
    }

    pub fn upgrade(&self) -> Option<DataCenterRef> {
        self.0.upgrade().map(DataCenterRef)
    }
}
