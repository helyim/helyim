use std::collections::HashMap;

use faststr::FastStr;
use futures::channel::mpsc::unbounded;
use helyim_macros::event_fn;
use rand::random;
use serde::Serialize;
use tokio::task::JoinHandle;

use crate::{
    errors::{Error, Result},
    rt_spawn,
    storage::VolumeId,
    topology::{data_node_loop, DataCenterEventTx, DataNode, DataNodeEventTx},
};

#[derive(Debug, Serialize)]
pub struct Rack {
    id: FastStr,
    #[serde(skip)]
    nodes: HashMap<FastStr, DataNodeEventTx>,
    max_volume_id: VolumeId,
    #[serde(skip)]
    data_center: Option<DataCenterEventTx>,
    #[serde(skip)]
    handles: Vec<JoinHandle<()>>,
    #[serde(skip)]
    shutdown: async_broadcast::Receiver<()>,
}

#[event_fn]
impl Rack {
    pub fn new(id: FastStr, shutdown: async_broadcast::Receiver<()>) -> Rack {
        Rack {
            id,
            nodes: HashMap::new(),
            max_volume_id: 0,
            data_center: None,
            handles: Vec::new(),
            shutdown,
        }
    }

    pub fn id(&self) -> FastStr {
        self.id.clone()
    }

    pub fn set_data_center(&mut self, data_center: DataCenterEventTx) {
        self.data_center = Some(data_center)
    }

    pub fn data_nodes(&self) -> HashMap<FastStr, DataNodeEventTx> {
        self.nodes.clone()
    }

    pub fn adjust_max_volume_id(&mut self, vid: VolumeId) -> Result<()> {
        if vid > self.max_volume_id {
            self.max_volume_id = vid;
        }

        if let Some(dc) = self.data_center.as_ref() {
            dc.adjust_max_volume_id(self.max_volume_id)?;
        }

        Ok(())
    }

    pub async fn get_or_create_data_node(
        &mut self,
        id: FastStr,
        ip: FastStr,
        port: u16,
        public_url: FastStr,
        max_volumes: i64,
    ) -> Result<DataNodeEventTx> {
        match self.nodes.get(&id) {
            Some(data_node) => Ok(data_node.clone()),
            None => {
                let data_node =
                    DataNode::new(id.clone(), ip, port, public_url, max_volumes).await?;
                let (tx, rx) = unbounded();
                self.handles.push(rt_spawn(data_node_loop(
                    data_node,
                    rx,
                    self.shutdown.clone(),
                )));

                let data_node = DataNodeEventTx::new(tx);
                self.nodes.insert(id, data_node.clone());
                Ok(data_node)
            }
        }
    }

    pub async fn data_center_id(&self) -> Result<FastStr> {
        match self.data_center.as_ref() {
            Some(data_center) => data_center.id().await,
            None => Ok(FastStr::empty()),
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
