use std::{
    collections::{HashMap, HashSet},
    sync::Weak,
};

use futures::{
    channel::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    lock::Mutex,
    StreamExt,
};
use serde::Serialize;

use crate::{
    directory::topology::rack::Rack,
    errors::Result,
    storage::{VolumeId, VolumeInfo},
};

#[derive(Debug, Default, Clone, Serialize)]
pub struct DataNode {
    pub id: String,
    pub ip: String,
    pub port: i64,
    pub public_url: String,
    pub last_seen: i64,
    #[serde(skip)]
    pub rack: Weak<Mutex<Rack>>,
    pub volumes: HashMap<VolumeId, VolumeInfo>,
    pub max_volumes: i64,
    pub max_volume_id: VolumeId,
}

unsafe impl Send for DataNode {}

impl std::fmt::Display for DataNode {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "({}, volumes: {})", self.id, self.volumes.len())
    }
}

impl DataNode {
    pub fn new(id: &str, ip: &str, port: i64, public_url: &str, max_volumes: i64) -> DataNode {
        DataNode {
            id: String::from(id),
            ip: String::from(ip),
            port,
            public_url: String::from(public_url),
            last_seen: 0,
            rack: Weak::default(),
            volumes: HashMap::new(),
            max_volumes,
            max_volume_id: 0,
        }
    }

    pub fn url(&self) -> String {
        format!("{}:{}", self.ip, self.port)
    }

    pub async fn adjust_max_volume_id(&mut self, vid: VolumeId) {
        if vid > self.max_volume_id {
            self.max_volume_id = vid;
        }

        if let Some(rack) = self.rack.upgrade() {
            rack.lock()
                .await
                .adjust_max_volume_id(self.max_volume_id)
                .await;
        }
    }

    pub async fn add_or_update_volume(&mut self, v: VolumeInfo) {
        self.adjust_max_volume_id(v.id).await;
        self.volumes.insert(v.id, v);
    }

    pub fn has_volumes(&self) -> i64 {
        self.volumes.len() as i64
    }

    pub fn max_volumes(&self) -> i64 {
        self.max_volumes
    }

    pub fn free_volumes(&self) -> i64 {
        self.max_volumes() - self.has_volumes()
    }

    pub async fn rack_id(&self) -> String {
        match self.rack.upgrade() {
            Some(rack) => rack.lock().await.id.clone(),
            None => String::from(""),
        }
    }

    pub async fn data_center_id(&self) -> String {
        match self.rack.upgrade() {
            Some(rack) => rack.lock().await.data_center_id().await,
            None => String::from(""),
        }
    }

    pub async fn update_volumes(&mut self, infos: Vec<VolumeInfo>) -> Vec<VolumeInfo> {
        let mut volumes = HashSet::new();
        for info in infos.iter() {
            volumes.insert(info.id);
        }

        let mut deleted_id = vec![];
        let mut deleted = vec![];

        for (id, volume) in self.volumes.iter_mut() {
            if !volumes.contains(id) {
                deleted_id.push(volume.id)
            }
        }

        for vi in infos {
            self.add_or_update_volume(vi).await;
        }

        for id in deleted_id.iter() {
            if let Some(volume) = self.volumes.remove(id) {
                deleted.push(volume);
            }
        }

        deleted
    }
}

pub enum DataNodeEvent {
    HasVolumes(oneshot::Sender<i64>),
    MaxVolumes(oneshot::Sender<i64>),
    FreeVolumes(oneshot::Sender<i64>),
    Url(oneshot::Sender<String>),
    AddOrUpdateVolume(VolumeInfo),
    Ip(oneshot::Sender<String>),
    Port(oneshot::Sender<i64>),
    GetVolume(VolumeId, oneshot::Sender<Option<VolumeInfo>>),
    Id(oneshot::Sender<String>),
}

pub async fn data_node_loop(
    mut data_node: DataNode,
    mut data_node_rx: UnboundedReceiver<DataNodeEvent>,
) {
    while let Some(event) = data_node_rx.next().await {
        match event {
            DataNodeEvent::HasVolumes(tx) => {
                let _ = tx.send(data_node.has_volumes());
            }
            DataNodeEvent::MaxVolumes(tx) => {
                let _ = tx.send(data_node.max_volumes());
            }
            DataNodeEvent::FreeVolumes(tx) => {
                let _ = tx.send(data_node.free_volumes());
            }
            DataNodeEvent::Url(tx) => {
                let _ = tx.send(data_node.url());
            }
            DataNodeEvent::AddOrUpdateVolume(v) => {
                data_node.add_or_update_volume(v).await;
            }
            DataNodeEvent::Ip(tx) => {
                let _ = tx.send(data_node.ip.clone());
            }
            DataNodeEvent::Port(tx) => {
                let _ = tx.send(data_node.port);
            }
            DataNodeEvent::GetVolume(vid, tx) => {
                let _ = tx.send(data_node.volumes.get(&vid).cloned());
            }
            DataNodeEvent::Id(tx) => {
                let _ = tx.send(data_node.id.clone());
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct DataNodeEventTx(UnboundedSender<DataNodeEvent>);

impl DataNodeEventTx {
    pub fn new(tx: UnboundedSender<DataNodeEvent>) -> Self {
        DataNodeEventTx(tx)
    }

    pub async fn has_volumes(&self) -> Result<i64> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(DataNodeEvent::HasVolumes(tx))?;
        Ok(rx.await?)
    }

    pub async fn max_volumes(&self) -> Result<i64> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(DataNodeEvent::MaxVolumes(tx))?;
        Ok(rx.await?)
    }

    pub async fn free_volumes(&self) -> Result<i64> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(DataNodeEvent::FreeVolumes(tx))?;
        Ok(rx.await?)
    }

    pub async fn url(&self) -> Result<String> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(DataNodeEvent::Url(tx))?;
        Ok(rx.await?)
    }

    pub async fn add_or_update_volume(&self, v: VolumeInfo) -> Result<()> {
        self.0.unbounded_send(DataNodeEvent::AddOrUpdateVolume(v))?;
        Ok(())
    }

    pub async fn ip(&self) -> Result<String> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(DataNodeEvent::Ip(tx))?;
        Ok(rx.await?)
    }

    pub async fn port(&self) -> Result<i64> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(DataNodeEvent::Port(tx))?;
        Ok(rx.await?)
    }

    pub async fn get_volume(&self, vid: VolumeId) -> Result<Option<VolumeInfo>> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(DataNodeEvent::GetVolume(vid, tx))?;
        Ok(rx.await?)
    }

    pub async fn id(&self) -> Result<String> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(DataNodeEvent::Id(tx))?;
        Ok(rx.await?)
    }
}
