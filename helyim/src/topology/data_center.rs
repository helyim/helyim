use std::{collections::HashMap, sync::Arc};

use faststr::FastStr;
use helyim_macros::event_fn;
use rand::random;
use serde::Serialize;

use crate::{
    errors::{Error, Result},
    storage::VolumeId,
    topology::{DataNode, Rack},
};

#[derive(Debug, Serialize)]
pub struct DataCenter {
    id: FastStr,
    max_volume_id: VolumeId,
    #[serde(skip)]
    racks: HashMap<FastStr, Arc<Rack>>,
    #[serde(skip)]
    shutdown: async_broadcast::Receiver<()>,
}

#[event_fn]
impl DataCenter {
    pub fn new(id: FastStr, shutdown: async_broadcast::Receiver<()>) -> DataCenter {
        DataCenter {
            id,
            racks: HashMap::new(),
            max_volume_id: 0,
            shutdown,
        }
    }
    pub fn id(&self) -> FastStr {
        self.id.clone()
    }

    pub fn max_volume_id(&self) -> VolumeId {
        self.max_volume_id
    }

    pub fn racks(&self) -> HashMap<FastStr, Arc<Rack>> {
        self.racks.clone()
    }

    pub fn adjust_max_volume_id(&mut self, vid: VolumeId) {
        if vid > self.max_volume_id {
            self.max_volume_id = vid;
        }
    }

    pub fn get_or_create_rack(&mut self, id: FastStr) -> Arc<Rack> {
        match self.racks.get(&id) {
            Some(rack) => rack.clone(),
            None => {
                let rack = Rack::new(id.clone(), self.shutdown.clone());
                let rack = Arc::new(rack);
                self.racks.insert(id, rack.clone());
                rack
            }
        }
    }

    pub async fn has_volumes(&self) -> Result<i64> {
        let mut ret = 0;
        for rack_tx in self.racks.values() {
            ret += rack_tx.has_volumes().await?;
        }
        Ok(ret)
    }

    pub async fn max_volumes(&self) -> Result<i64> {
        let mut ret = 0;
        for rack_tx in self.racks.values() {
            ret += rack_tx.max_volumes().await?;
        }
        Ok(ret)
    }

    pub async fn free_volumes(&self) -> Result<i64> {
        let mut free_volumes = 0;
        for rack_tx in self.racks.values() {
            free_volumes += rack_tx.free_volumes().await?;
        }
        Ok(free_volumes)
    }

    pub async fn reserve_one_volume(&self) -> Result<Arc<DataNode>> {
        // randomly select one
        let mut free_volumes = 0;
        for (_, rack) in self.racks.iter() {
            free_volumes += rack.free_volumes().await?;
        }

        let idx = random::<u32>() as i64 % free_volumes;

        for (_, rack) in self.racks.iter() {
            free_volumes -= rack.free_volumes().await?;
            if free_volumes == idx {
                return rack.reserve_one_volume().await;
            }
        }

        Err(Error::NoFreeSpace(format!(
            "no free volumes found on data center {}",
            self.id
        )))
    }
}
