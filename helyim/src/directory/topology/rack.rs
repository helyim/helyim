use std::{
    collections::HashMap,
    sync::{Arc, Weak},
};

use futures::{
    cargStreamExt,
    channel::{
        mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    lock::Mutex,
};
use rand::random;
use serde::Serialize;

use crate::{
    directory::topology::{data_node_loop, DataCenter, DataCenterEvent, DataNode, DataNodeEvent},
    errors::{Error, Result},
    rt_spawn,
    storage::VolumeId,
};

#[derive(Clone, Debug, Serialize)]
pub struct Rack {
    pub id: String,
    #[serde(skip)]
    pub nodes: HashMap<String, Arc<Mutex<DataNode>>>,
    #[serde(skip)]
    pub nodes_tx: HashMap<String, UnboundedSender<DataNodeEvent>>,
    pub max_volume_id: VolumeId,
    #[serde(skip)]
    pub data_center: Weak<Mutex<DataCenter>>,
    #[serde(skip)]
    pub data_center_tx: Option<UnboundedSender<DataCenterEvent>>,
}

impl Rack {
    pub fn new(id: &str) -> Rack {
        Rack {
            id: String::from(id),
            nodes: HashMap::new(),
            nodes_tx: HashMap::new(),
            data_center: Weak::new(),
            max_volume_id: 0,
            data_center_tx: None,
        }
    }

    pub async fn adjust_max_volume_id(&mut self, vid: VolumeId) {
        if vid > self.max_volume_id {
            self.max_volume_id = vid;
        }

        if let Some(dc) = self.data_center.upgrade() {
            dc.lock().await.adjust_max_volume_id(self.max_volume_id);
        }
    }

    #[deprecated]
    pub fn get_or_create_data_node(
        &mut self,
        id: &str,
        ip: &str,
        port: i64,
        public_url: &str,
        max_volumes: i64,
    ) -> Arc<Mutex<DataNode>> {
        let node = self
            .nodes
            .entry(String::from(id))
            .or_insert(Arc::new(Mutex::new(DataNode::new(
                id,
                ip,
                port,
                public_url,
                max_volumes,
            ))));
        node.clone()
    }

    pub fn get_or_create_data_node_tx(
        &mut self,
        id: &str,
        ip: &str,
        port: i64,
        public_url: &str,
        max_volumes: i64,
    ) -> UnboundedSender<DataNodeEvent> {
        self.nodes_tx
            .entry(String::from(id))
            .or_insert_with(|| {
                let data_node = DataNode::new(id, ip, port, public_url, max_volumes);
                let (tx, rx) = unbounded();
                rt_spawn(data_node_loop(data_node, rx));
                tx
            })
            .clone()
    }

    pub async fn data_center_id(&self) -> String {
        match self.data_center.upgrade() {
            Some(data_center) => data_center.lock().await.id.clone(),
            None => String::from(""),
        }
    }

    #[deprecated]
    pub async fn has_volumes(&self) -> i64 {
        let mut count = 0;
        for dn in self.nodes.values() {
            count += dn.lock().await.has_volumes();
        }
        count
    }

    pub async fn has_volumes_tx(&self) -> Result<i64> {
        let mut count = 0;
        for dn_tx in self.nodes_tx.values() {
            let (tx, rx) = oneshot::channel();
            let _ = dn_tx.unbounded_send(DataNodeEvent::HasVolumes(tx));
            count += rx.await?;
        }
        Ok(count)
    }

    #[deprecated]
    pub async fn max_volumes(&self) -> i64 {
        let mut max_volumes = 0;
        for dn in self.nodes.values() {
            max_volumes += dn.lock().await.max_volumes();
        }
        max_volumes
    }

    pub async fn max_volumes_tx(&self) -> Result<i64> {
        let mut max_volumes = 0;
        for dn_tx in self.nodes_tx.values() {
            let (tx, rx) = oneshot::channel();
            let _ = dn_tx.unbounded_send(DataNodeEvent::MaxVolumes(tx));
            max_volumes += rx.await?;
        }
        Ok(max_volumes)
    }

    #[deprecated]
    pub async fn free_volumes(&self) -> i64 {
        let mut free_volumes = 0;
        for dn in self.nodes.values() {
            free_volumes += dn.lock().await.free_volumes();
        }
        free_volumes
    }

    pub async fn free_volumes_tx(&self) -> Result<i64> {
        let mut free_volumes = 0;
        for dn_tx in self.nodes_tx.values() {
            let (tx, rx) = oneshot::channel();
            let _ = dn_tx.unbounded_send(DataNodeEvent::FreeVolumes(tx));
            free_volumes += rx.await?;
        }
        Ok(free_volumes)
    }

    pub async fn reserve_one_volume(&self) -> Result<Arc<Mutex<DataNode>>> {
        // randomly select
        let mut free_volumes = 0;
        for (_, dn) in self.nodes.iter() {
            free_volumes += dn.lock().await.free_volumes();
        }

        let idx = random::<u32>() as i64 % free_volumes;

        for (_, dn) in self.nodes.iter() {
            free_volumes -= dn.lock().await.free_volumes();
            if free_volumes == idx {
                return Ok(dn.clone());
            }
        }

        Err(Error::NoFreeSpace(format!(
            "reserve_one_volume on rack {} fail",
            self.id
        )))
    }
}

pub enum RackEvent {
    HasVolumes(oneshot::Sender<Result<i64>>),
    MaxVolumes(oneshot::Sender<Result<i64>>),
    FreeVolumes(oneshot::Sender<Result<i64>>),
    SetDataCenter(UnboundedSender<DataCenterEvent>),
}

pub async fn rack_loop(mut rack: Rack, mut rack_rx: UnboundedReceiver<RackEvent>) {
    while let Some(event) = rack_rx.next().await {
        match event {
            RackEvent::HasVolumes(tx) => {
                let _ = tx.send(rack.has_volumes_tx().await);
            }
            RackEvent::MaxVolumes(tx) => {
                let _ = tx.send(rack.max_volumes_tx().await);
            }
            RackEvent::FreeVolumes(tx) => {
                let _ = tx.send(rack.free_volumes_tx().await);
            }
            RackEvent::SetDataCenter(tx) => {
                rack.data_center_tx = Some(tx);
            }
        }
    }
}
