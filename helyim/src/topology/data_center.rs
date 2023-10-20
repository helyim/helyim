use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use dashmap::DashMap;
use faststr::FastStr;
use futures::channel::mpsc::unbounded;
use rand::random;
use serde::Serialize;

use crate::{
    errors::{Error, Result},
    rt_spawn,
    storage::VolumeId,
    topology::{rack_loop, DataNodeEventTx, Rack, RackEventTx},
};

#[derive(Debug, Serialize)]
pub struct DataCenter {
    pub id: FastStr,
    max_volume_id: AtomicU32,
    #[serde(skip)]
    pub racks: Arc<DashMap<FastStr, RackEventTx>>,
    #[serde(skip)]
    shutdown: async_broadcast::Receiver<()>,
}

impl DataCenter {
    pub fn new(id: FastStr, shutdown: async_broadcast::Receiver<()>) -> DataCenter {
        DataCenter {
            id,
            racks: Arc::new(DashMap::new()),
            max_volume_id: AtomicU32::new(0),
            shutdown,
        }
    }

    pub fn max_volume_id(&self) -> VolumeId {
        self.max_volume_id.load(Ordering::Relaxed)
    }

    pub fn racks(&self) -> Arc<DashMap<FastStr, RackEventTx>> {
        self.racks.clone()
    }

    pub fn adjust_max_volume_id(&self, vid: VolumeId) {
        if vid > self.max_volume_id.load(Ordering::Relaxed) {
            self.max_volume_id.store(vid, Ordering::Relaxed);
        }
    }

    pub fn get_or_create_rack(&self, id: FastStr) -> RackEventTx {
        self.racks
            .entry(id.clone())
            .or_insert_with(|| {
                let (tx, rx) = unbounded();
                rt_spawn(rack_loop(
                    Rack::new(id, self.shutdown.clone()),
                    rx,
                    self.shutdown.clone(),
                ));
                RackEventTx::new(tx)
            })
            .clone()
    }

    pub async fn has_volumes(&self) -> Result<i64> {
        let mut ret = 0;
        for rack_tx in self.racks.iter() {
            ret += rack_tx.has_volumes().await?;
        }
        Ok(ret)
    }

    pub async fn max_volumes(&self) -> Result<i64> {
        let mut ret = 0;
        for rack_tx in self.racks.iter() {
            ret += rack_tx.max_volumes().await?;
        }
        Ok(ret)
    }

    pub async fn free_volumes(&self) -> Result<i64> {
        let mut free_volumes = 0;
        for rack_tx in self.racks.iter() {
            free_volumes += rack_tx.free_volumes().await?;
        }
        Ok(free_volumes)
    }

    pub async fn reserve_one_volume(&self) -> Result<DataNodeEventTx> {
        // randomly select one
        let mut free_volumes = 0;
        for rack in self.racks.iter() {
            free_volumes += rack.free_volumes().await?;
        }

        let idx = random::<u32>() as i64 % free_volumes;

        for rack in self.racks.iter() {
            free_volumes -= rack.free_volumes().await?;
            if free_volumes == idx {
                return rack.reserve_one_volume().await;
            }
        }

        Err(Error::NoFreeSpace(format!(
            "reserve_one_volume on dc {} fail",
            self.id
        )))
    }
}
