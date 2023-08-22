use std::{
    collections::{HashMap, HashSet},
    sync::Weak,
};

use futures::{
    channel::{mpsc::UnboundedReceiver, oneshot},
    lock::Mutex,
    StreamExt,
};
use serde::Serialize;

use crate::{
    directory::topology::rack::Rack,
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

        let mut deleted_id: Vec<VolumeId> = vec![];
        let mut deleted: Vec<VolumeInfo> = vec![];

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
}

pub async fn data_node_loop(
    data_node: DataNode,
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
        }
    }
}
