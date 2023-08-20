use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Weak},
};

use futures::lock::Mutex;
use rand::random;
use serde::Serialize;

use crate::{
    errors::{Error, Result},
    storage::{VolumeId, VolumeInfo},
};

pub mod collection;
#[allow(clippy::module_inception)]
pub mod topology;
pub mod volume_grow;
pub mod volume_layout;

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

// TODO
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

#[derive(Clone, Debug, Serialize)]
pub struct Rack {
    pub id: String,
    #[serde(skip)]
    pub nodes: HashMap<String, Arc<Mutex<DataNode>>>,
    pub max_volume_id: VolumeId,
    #[serde(skip)]
    pub data_center: Weak<Mutex<DataCenter>>,
}

impl Rack {
    fn new(id: &str) -> Rack {
        Rack {
            id: String::from(id),
            nodes: HashMap::new(),
            data_center: Weak::new(),
            max_volume_id: 0,
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

    pub async fn data_center_id(&self) -> String {
        match self.data_center.upgrade() {
            Some(data_center) => data_center.lock().await.id.clone(),
            None => String::from(""),
        }
    }

    pub async fn has_volumes(&self) -> i64 {
        let mut count = 0;
        for dn in self.nodes.values() {
            count += dn.lock().await.has_volumes();
        }
        count
    }

    pub async fn max_volumes(&self) -> i64 {
        let mut max_volumes = 0;
        for dn in self.nodes.values() {
            max_volumes += dn.lock().await.max_volumes();
        }
        max_volumes
    }

    pub async fn free_volumes(&self) -> i64 {
        let mut free_volumes = 0;
        for dn in self.nodes.values() {
            free_volumes += dn.lock().await.free_volumes();
        }
        free_volumes
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

#[derive(Debug, Clone, Serialize)]
pub struct DataCenter {
    pub id: String,
    pub max_volume_id: VolumeId,
    #[serde(skip)]
    pub racks: HashMap<String, Arc<Mutex<Rack>>>,
}

impl DataCenter {
    fn new(id: &str) -> DataCenter {
        DataCenter {
            id: String::from(id),
            racks: HashMap::new(),
            max_volume_id: 0,
        }
    }

    pub fn adjust_max_volume_id(&mut self, vid: VolumeId) {
        if vid > self.max_volume_id {
            self.max_volume_id = vid;
        }
    }

    pub fn get_or_create_rack(&mut self, id: &str) -> Arc<Mutex<Rack>> {
        self.racks
            .entry(String::from(id))
            .or_insert(Arc::new(Mutex::new(Rack::new(id))))
            .clone()
    }

    pub async fn has_volumes(&self) -> i64 {
        let mut ret = 0;
        for rack in self.racks.values() {
            ret += rack.lock().await.has_volumes().await;
        }
        ret
    }

    pub async fn max_volumes(&self) -> i64 {
        let mut ret = 0;
        for rack in self.racks.values() {
            ret += rack.lock().await.max_volumes().await;
        }
        ret
    }

    pub async fn free_volumes(&self) -> i64 {
        let mut ret = 0;
        for rack in self.racks.values() {
            ret += rack.lock().await.free_volumes().await;
        }
        ret
    }

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
}
