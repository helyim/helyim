use std::collections::HashMap;

use faststr::FastStr;
use futures::{
    channel::mpsc::{unbounded, UnboundedReceiver},
    StreamExt,
};
use helyim_macros::event_fn;
use rand::random;
use serde::Serialize;
use tokio::task::JoinHandle;
use tracing::info;

use crate::{
    errors::{Error, Result},
    rt_spawn,
    storage::VolumeId,
    topology::{rack_loop, DataNodeEventTx, Rack, RackEventTx},
};

#[derive(Debug, Serialize)]
pub struct DataCenter {
    id: FastStr,
    max_volume_id: VolumeId,
    #[serde(skip)]
    racks: HashMap<FastStr, RackEventTx>,
    #[serde(skip)]
    handles: Vec<JoinHandle<()>>,
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
            handles: Vec::new(),
            shutdown,
        }
    }
    pub fn id(&self) -> FastStr {
        self.id.clone()
    }

    pub fn max_volume_id(&self) -> VolumeId {
        self.max_volume_id
    }

    pub fn racks(&self) -> HashMap<FastStr, RackEventTx> {
        self.racks.clone()
    }

    pub fn adjust_max_volume_id(&mut self, vid: VolumeId) {
        if vid > self.max_volume_id {
            self.max_volume_id = vid;
        }
    }

    pub fn get_or_create_rack(&mut self, id: FastStr) -> RackEventTx {
        self.racks
            .entry(id.clone())
            .or_insert_with(|| {
                let (tx, rx) = unbounded();
                self.handles.push(rt_spawn(rack_loop(
                    Rack::new(id, self.shutdown.clone()),
                    rx,
                )));

                RackEventTx::new(tx)
            })
            .clone()
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

    pub async fn reserve_one_volume(&self) -> Result<DataNodeEventTx> {
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
            "reserve_one_volume on dc {} fail",
            self.id
        )))
    }
}

pub async fn data_center_loop(
    mut data_center: DataCenter,
    mut data_center_rx: UnboundedReceiver<DataCenterEvent>,
) {
    info!("data center [{}] event loop starting.", data_center.id);
    loop {
        tokio::select! {
            Some(event) = data_center_rx.next() => {
                match event {
                    DataCenterEvent::HasVolumes {tx} => {
                        let _ = tx.send(data_center.has_volumes().await);
                    }
                    DataCenterEvent::MaxVolumes {tx} => {
                        let _ = tx.send(data_center.max_volumes().await);
                    }
                    DataCenterEvent::FreeVolumes{tx} => {
                        let _ = tx.send(data_center.free_volumes().await);
                    }
                    DataCenterEvent::MaxVolumeId{tx} => {
                        let _ = tx.send(data_center.max_volume_id());
                    }
                    DataCenterEvent::Id{tx} => {
                        let _ = tx.send(data_center.id());
                    }
                    DataCenterEvent::Racks{tx} => {
                        let _ = tx.send(data_center.racks());
                    }
                    DataCenterEvent::ReserveOneVolume{tx} => {
                        let _ = tx.send(data_center.reserve_one_volume().await);
                    }
                    DataCenterEvent::GetOrCreateRack{id, tx} => {
                        let _ = tx.send(data_center.get_or_create_rack(id));
                    }
                    DataCenterEvent::AdjustMaxVolumeId{vid} => {
                        data_center.adjust_max_volume_id(vid);
                    }
                }
            }
            _ = data_center.shutdown.recv() => {
                break;
            }
        }
    }
    info!("data center [{}] event loop stopped.", data_center.id);
}
