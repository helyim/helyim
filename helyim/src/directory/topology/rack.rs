use std::collections::HashMap;

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
    directory::topology::{
        data_center::DataCenterEventTx, data_node_loop, DataNode, DataNodeEventTx,
    },
    errors::{Error, Result},
    rt_spawn,
    storage::VolumeId,
};

#[derive(Debug, Serialize)]
pub struct Rack {
    pub id: String,
    #[serde(skip)]
    pub nodes: HashMap<String, DataNodeEventTx>,
    pub max_volume_id: VolumeId,
    #[serde(skip)]
    pub data_center: Option<DataCenterEventTx>,
    #[serde(skip)]
    pub handles: Vec<JoinHandle<()>>,
}

impl Rack {
    pub fn new(id: &str) -> Rack {
        Rack {
            id: String::from(id),
            nodes: HashMap::new(),
            max_volume_id: 0,
            data_center: None,
            handles: Vec::new(),
        }
    }

    pub async fn adjust_max_volume_id(&mut self, vid: VolumeId) -> Result<()> {
        if vid > self.max_volume_id {
            self.max_volume_id = vid;
        }

        if let Some(dc) = self.data_center.as_ref() {
            dc.adjust_max_volume_id(self.max_volume_id).await?;
        }

        Ok(())
    }

    pub fn get_or_create_data_node(
        &mut self,
        id: &str,
        ip: &str,
        port: u32,
        public_url: &str,
        max_volumes: i64,
    ) -> DataNodeEventTx {
        self.nodes
            .entry(String::from(id))
            .or_insert_with(|| {
                let data_node = DataNode::new(id, ip, port, public_url, max_volumes);
                let (tx, rx) = unbounded();
                self.handles.push(rt_spawn(data_node_loop(data_node, rx)));

                DataNodeEventTx::new(tx)
            })
            .clone()
    }

    pub async fn data_center_id(&self) -> Result<String> {
        match self.data_center.as_ref() {
            Some(data_center) => data_center.id().await,
            None => Ok(String::from("")),
        }
    }

    pub async fn has_volumes(&self) -> Result<i64> {
        let mut count = 0;
        for dn_tx in self.nodes.values() {
            count += dn_tx.has_volumes().await?;
        }
        Ok(count)
    }

    pub async fn max_volumes(&self) -> Result<i64> {
        let mut max_volumes = 0;
        for dn_tx in self.nodes.values() {
            max_volumes += dn_tx.max_volumes().await?;
        }
        Ok(max_volumes)
    }

    pub async fn free_volumes(&self) -> Result<i64> {
        let mut free_volumes = 0;
        for dn_tx in self.nodes.values() {
            free_volumes += dn_tx.free_volumes().await?;
        }
        Ok(free_volumes)
    }

    pub async fn reserve_one_volume(&self) -> Result<DataNodeEventTx> {
        // randomly select
        let mut free_volumes = 0;
        for (_, dn_tx) in self.nodes.iter() {
            free_volumes += dn_tx.free_volumes().await?;
        }

        let idx = random::<u32>() as i64 % free_volumes;

        for (_, dn_tx) in self.nodes.iter() {
            free_volumes -= dn_tx.free_volumes().await?;
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

pub enum RackEvent {
    HasVolumes(oneshot::Sender<Result<i64>>),
    MaxVolumes(oneshot::Sender<Result<i64>>),
    FreeVolumes(oneshot::Sender<Result<i64>>),
    SetDataCenter(DataCenterEventTx),
    ReserveOneVolume(oneshot::Sender<Result<DataNodeEventTx>>),
    DataNodes(oneshot::Sender<HashMap<String, DataNodeEventTx>>),
    Id(oneshot::Sender<String>),
    DataCenterId(oneshot::Sender<Result<String>>),
    GetOrCreateDataNode {
        id: String,
        ip: String,
        port: u32,
        public_url: String,
        max_volumes: i64,
        tx: oneshot::Sender<DataNodeEventTx>,
    },
    AdjustMaxVolumeId(VolumeId, oneshot::Sender<Result<()>>),
}

pub async fn rack_loop(mut rack: Rack, mut rack_rx: UnboundedReceiver<RackEvent>) {
    info!("rack [{}] event loop starting.", rack.id);
    while let Some(event) = rack_rx.next().await {
        match event {
            RackEvent::HasVolumes(tx) => {
                let _ = tx.send(rack.has_volumes().await);
            }
            RackEvent::MaxVolumes(tx) => {
                let _ = tx.send(rack.max_volumes().await);
            }
            RackEvent::FreeVolumes(tx) => {
                let _ = tx.send(rack.free_volumes().await);
            }
            RackEvent::SetDataCenter(tx) => {
                rack.data_center = Some(tx);
            }
            RackEvent::ReserveOneVolume(tx) => {
                let _ = tx.send(rack.reserve_one_volume().await);
            }
            RackEvent::DataNodes(tx) => {
                let _ = tx.send(rack.nodes.clone());
            }
            RackEvent::Id(tx) => {
                let _ = tx.send(rack.id.clone());
            }
            RackEvent::DataCenterId(tx) => {
                let _ = tx.send(rack.data_center_id().await);
            }
            RackEvent::GetOrCreateDataNode {
                id,
                ip,
                port,
                public_url,
                max_volumes,
                tx,
            } => {
                let _ =
                    tx.send(rack.get_or_create_data_node(&id, &ip, port, &public_url, max_volumes));
            }
            RackEvent::AdjustMaxVolumeId(vid, tx) => {
                let _ = tx.send(rack.adjust_max_volume_id(vid).await);
            }
        }
    }

    if let Some(data_center) = rack.data_center.as_ref() {
        data_center.close();
    }

    for (_, node) in rack.nodes.iter() {
        node.close();
    }

    info!("rack [{}] event loop stopping.", rack.id);
}

#[derive(Debug, Clone)]
pub struct RackEventTx(UnboundedSender<RackEvent>);

impl RackEventTx {
    pub fn new(tx: UnboundedSender<RackEvent>) -> Self {
        RackEventTx(tx)
    }

    pub async fn has_volumes(&self) -> Result<i64> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(RackEvent::HasVolumes(tx))?;
        rx.await?
    }

    pub async fn max_volumes(&self) -> Result<i64> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(RackEvent::MaxVolumes(tx))?;
        rx.await?
    }

    pub async fn free_volumes(&self) -> Result<i64> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(RackEvent::FreeVolumes(tx))?;
        rx.await?
    }

    pub fn set_data_center(&self, data_center: DataCenterEventTx) -> Result<()> {
        self.0
            .unbounded_send(RackEvent::SetDataCenter(data_center))?;
        Ok(())
    }

    pub async fn reserve_one_volume(&self) -> Result<DataNodeEventTx> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(RackEvent::ReserveOneVolume(tx))?;
        rx.await?
    }

    pub async fn data_nodes(&self) -> Result<HashMap<String, DataNodeEventTx>> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(RackEvent::DataNodes(tx))?;
        Ok(rx.await?)
    }

    pub async fn id(&self) -> Result<String> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(RackEvent::Id(tx))?;
        Ok(rx.await?)
    }

    pub async fn data_center_id(&self) -> Result<String> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(RackEvent::DataCenterId(tx))?;
        rx.await?
    }

    pub async fn get_or_create_data_node(
        &self,
        id: String,
        ip: String,
        port: u32,
        public_url: String,
        max_volumes: i64,
    ) -> Result<DataNodeEventTx> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(RackEvent::GetOrCreateDataNode {
            id,
            ip,
            port,
            public_url,
            max_volumes,
            tx,
        })?;
        Ok(rx.await?)
    }

    pub async fn adjust_max_volume_id(&self, vid: VolumeId) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0
            .unbounded_send(RackEvent::AdjustMaxVolumeId(vid, tx))?;
        rx.await?
    }

    pub fn close(&self) {
        self.0.close_channel();
    }
}
