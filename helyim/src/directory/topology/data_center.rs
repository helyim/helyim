use std::{collections::HashMap, sync::Arc};

use futures::{
    channel::{
        mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    lock::Mutex,
    StreamExt,
};
use rand::random;
use serde::Serialize;

use crate::{
    directory::topology::{data_node::DataNode, rack_loop, DataNodeEventTx, Rack, RackEventTx},
    errors::{Error, Result},
    rt_spawn,
    storage::VolumeId,
};

#[derive(Debug, Clone, Serialize)]
pub struct DataCenter {
    pub id: String,
    pub max_volume_id: VolumeId,
    #[serde(skip)]
    pub racks: HashMap<String, Arc<Mutex<Rack>>>,
    #[serde(skip)]
    pub racks_tx: HashMap<String, RackEventTx>,
}

impl DataCenter {
    pub fn new(id: &str) -> DataCenter {
        DataCenter {
            id: String::from(id),
            racks: HashMap::new(),
            racks_tx: HashMap::new(),
            max_volume_id: 0,
        }
    }

    pub fn adjust_max_volume_id(&mut self, vid: VolumeId) {
        if vid > self.max_volume_id {
            self.max_volume_id = vid;
        }
    }

    #[deprecated]
    pub fn get_or_create_rack(&mut self, id: &str) -> Arc<Mutex<Rack>> {
        self.racks
            .entry(String::from(id))
            .or_insert(Arc::new(Mutex::new(Rack::new(id))))
            .clone()
    }

    pub fn get_or_create_rack_tx(&mut self, id: &str) -> RackEventTx {
        self.racks_tx
            .entry(String::from(id))
            .or_insert_with(|| {
                let (tx, rx) = unbounded();
                rt_spawn(rack_loop(Rack::new(id), rx));
                RackEventTx::new(tx)
            })
            .clone()
    }

    #[deprecated]
    pub async fn has_volumes(&self) -> i64 {
        let mut ret = 0;
        for rack in self.racks.values() {
            ret += rack.lock().await.has_volumes().await;
        }
        ret
    }

    pub async fn has_volumes_tx(&self) -> Result<i64> {
        let mut ret = 0;
        for rack_tx in self.racks_tx.values() {
            ret += rack_tx.has_volumes().await?;
        }
        Ok(ret)
    }

    #[deprecated]
    pub async fn max_volumes(&self) -> i64 {
        let mut ret = 0;
        for rack in self.racks.values() {
            ret += rack.lock().await.max_volumes().await;
        }
        ret
    }

    pub async fn max_volumes_tx(&self) -> Result<i64> {
        let mut ret = 0;
        for rack_tx in self.racks_tx.values() {
            ret += rack_tx.max_volumes().await?;
        }
        Ok(ret)
    }

    #[deprecated]
    pub async fn free_volumes(&self) -> i64 {
        let mut ret = 0;
        for rack in self.racks.values() {
            ret += rack.lock().await.free_volumes().await;
        }
        ret
    }

    pub async fn free_volumes_tx(&self) -> Result<i64> {
        let mut free_volumes = 0;
        for rack_tx in self.racks_tx.values() {
            free_volumes += rack_tx.free_volumes().await?;
        }
        Ok(free_volumes)
    }

    #[deprecated]
    pub async fn reserve_one_volume(&self) -> Result<Arc<Mutex<DataNode>>> {
        // randomly select one
        let mut free_volumes = 0;
        for (_, rack) in self.racks.iter() {
            free_volumes += rack.lock().await.free_volumes().await;
        }

        let idx = random::<u32>() as i64 % free_volumes;

        for (_, rack) in self.racks.iter() {
            free_volumes -= rack.lock().await.free_volumes().await;
            if free_volumes == idx {
                return rack.lock().await.reserve_one_volume().await;
            }
        }

        Err(Error::NoFreeSpace(format!(
            "reserve_one_volume on dc {} fail",
            self.id
        )))
    }

    pub async fn reserve_one_volume2(&self) -> Result<DataNodeEventTx> {
        // randomly select one
        let mut free_volumes = 0;
        for (_, rack) in self.racks_tx.iter() {
            free_volumes += rack.free_volumes().await?;
        }

        let idx = random::<u32>() as i64 % free_volumes;

        for (_, rack) in self.racks_tx.iter() {
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
    Id(oneshot::Sender<String>),
    Racks(oneshot::Sender<HashMap<String, RackEventTx>>),
    ReserveOneVolume(oneshot::Sender<Result<DataNodeEventTx>>),
}

pub async fn data_center_loop(
    data_center: DataCenter,
    mut data_center_rx: UnboundedReceiver<DataCenterEvent>,
) {
    while let Some(event) = data_center_rx.next().await {
        match event {
            DataCenterEvent::HasVolumes(tx) => {
                let _ = tx.send(data_center.has_volumes_tx().await);
            }
            DataCenterEvent::MaxVolumes(tx) => {
                let _ = tx.send(data_center.max_volumes_tx().await);
            }
            DataCenterEvent::FreeVolumes(tx) => {
                let _ = tx.send(data_center.free_volumes_tx().await);
            }
            DataCenterEvent::MaxVolumeId(tx) => {
                let _ = tx.send(data_center.max_volume_id);
            }
            DataCenterEvent::Id(tx) => {
                let _ = tx.send(data_center.id.clone());
            }
            DataCenterEvent::Racks(tx) => {
                let _ = tx.send(data_center.racks_tx.clone());
            }
            DataCenterEvent::ReserveOneVolume(tx) => {
                let _ = tx.send(data_center.reserve_one_volume2().await);
            }
        }
    }
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
        Ok(rx.await??)
    }

    pub async fn max_volumes(&self) -> Result<i64> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(DataCenterEvent::MaxVolumes(tx))?;
        Ok(rx.await??)
    }

    pub async fn free_volumes(&self) -> Result<i64> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(DataCenterEvent::FreeVolumes(tx))?;
        Ok(rx.await??)
    }

    pub async fn max_volume_id(&self) -> Result<VolumeId> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(DataCenterEvent::MaxVolumeId(tx))?;
        Ok(rx.await?)
    }

    pub async fn id(&self) -> Result<String> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(DataCenterEvent::Id(tx))?;
        Ok(rx.await?)
    }

    pub async fn racks(&self) -> Result<HashMap<String, RackEventTx>> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(DataCenterEvent::Racks(tx))?;
        Ok(rx.await?)
    }

    pub async fn reserve_one_volume(&self) -> Result<DataNodeEventTx> {
        let (tx, rx) = oneshot::channel();
        self.0
            .unbounded_send(DataCenterEvent::ReserveOneVolume(tx))?;
        Ok(rx.await??)
    }
}
