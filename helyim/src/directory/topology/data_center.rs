use std::collections::HashMap;

use faststr::FastStr;
use futures::{
    channel::{
        mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    StreamExt,
};
use rand::random;
use serde::Serialize;
use tokio::task::JoinHandle;
use tracing::info;

use crate::{
    directory::topology::{rack_loop, DataNodeEventTx, Rack, RackEventTx},
    errors::{Error, Result},
    rt_spawn,
    storage::VolumeId,
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

pub enum DataCenterEvent {
    HasVolumes(oneshot::Sender<Result<i64>>),
    MaxVolumes(oneshot::Sender<Result<i64>>),
    FreeVolumes(oneshot::Sender<Result<i64>>),
    MaxVolumeId(oneshot::Sender<VolumeId>),
    Id(oneshot::Sender<FastStr>),
    Racks(oneshot::Sender<HashMap<FastStr, RackEventTx>>),
    ReserveOneVolume(oneshot::Sender<Result<DataNodeEventTx>>),
    GetOrCreateRack(FastStr, oneshot::Sender<RackEventTx>),
    AdjustMaxVolumeId(VolumeId),
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
                    DataCenterEvent::HasVolumes(tx) => {
                        let _ = tx.send(data_center.has_volumes().await);
                    }
                    DataCenterEvent::MaxVolumes(tx) => {
                        let _ = tx.send(data_center.max_volumes().await);
                    }
                    DataCenterEvent::FreeVolumes(tx) => {
                        let _ = tx.send(data_center.free_volumes().await);
                    }
                    DataCenterEvent::MaxVolumeId(tx) => {
                        let _ = tx.send(data_center.max_volume_id);
                    }
                    DataCenterEvent::Id(tx) => {
                        let _ = tx.send(data_center.id.clone());
                    }
                    DataCenterEvent::Racks(tx) => {
                        let _ = tx.send(data_center.racks.clone());
                    }
                    DataCenterEvent::ReserveOneVolume(tx) => {
                        let _ = tx.send(data_center.reserve_one_volume().await);
                    }
                    DataCenterEvent::GetOrCreateRack(id, tx) => {
                        let _ = tx.send(data_center.get_or_create_rack(id));
                    }
                    DataCenterEvent::AdjustMaxVolumeId(vid) => {
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

#[derive(Debug, Clone)]
pub struct DataCenterEventTx(UnboundedSender<DataCenterEvent>);

impl DataCenterEventTx {
    pub fn new(tx: UnboundedSender<DataCenterEvent>) -> Self {
        DataCenterEventTx(tx)
    }

    pub async fn has_volumes(&self) -> Result<i64> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(DataCenterEvent::HasVolumes(tx))?;
        rx.await?
    }

    pub async fn max_volumes(&self) -> Result<i64> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(DataCenterEvent::MaxVolumes(tx))?;
        rx.await?
    }

    pub async fn free_volumes(&self) -> Result<i64> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(DataCenterEvent::FreeVolumes(tx))?;
        rx.await?
    }

    pub async fn max_volume_id(&self) -> Result<VolumeId> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(DataCenterEvent::MaxVolumeId(tx))?;
        Ok(rx.await?)
    }

    pub async fn id(&self) -> Result<FastStr> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(DataCenterEvent::Id(tx))?;
        Ok(rx.await?)
    }

    pub async fn racks(&self) -> Result<HashMap<FastStr, RackEventTx>> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(DataCenterEvent::Racks(tx))?;
        Ok(rx.await?)
    }

    pub async fn reserve_one_volume(&self) -> Result<DataNodeEventTx> {
        let (tx, rx) = oneshot::channel();
        self.0
            .unbounded_send(DataCenterEvent::ReserveOneVolume(tx))?;
        rx.await?
    }

    pub async fn get_or_create_rack(&self, id: FastStr) -> Result<RackEventTx> {
        let (tx, rx) = oneshot::channel();
        self.0
            .unbounded_send(DataCenterEvent::GetOrCreateRack(id, tx))?;
        Ok(rx.await?)
    }

    pub async fn adjust_max_volume_id(&self, vid: VolumeId) -> Result<()> {
        self.0
            .unbounded_send(DataCenterEvent::AdjustMaxVolumeId(vid))?;
        Ok(())
    }
}
