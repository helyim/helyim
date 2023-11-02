use std::{collections::HashMap, sync::Arc};

use faststr::FastStr;
use helyim_macros::event_fn;
use rand::random;
use serde::Serialize;

use crate::{
    errors::{Error, Result},
    storage::VolumeId,
    topology::{DataCenterEventTx, DataNode},
};

#[derive(Debug, Serialize)]
pub struct Rack {
    id: FastStr,
    #[serde(skip)]
    nodes: HashMap<FastStr, Arc<DataNode>>,
    max_volume_id: VolumeId,
    #[serde(skip)]
    data_center: Option<DataCenterEventTx>,
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
            shutdown,
        }
    }

    pub fn id(&self) -> FastStr {
        self.id.clone()
    }

    pub fn set_data_center(&mut self, data_center: DataCenterEventTx) {
        self.data_center = Some(data_center)
    }

    pub fn data_nodes(&self) -> HashMap<FastStr, Arc<DataNode>> {
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
    ) -> Result<Arc<DataNode>> {
        match self.nodes.get(&id) {
            Some(data_node) => Ok(data_node.clone()),
            None => {
                let data_node = DataNode::new(
                    id.clone(),
                    ip,
                    port,
                    public_url,
                    max_volumes,
                    self.shutdown.clone(),
                )
                .await?;

                let data_node = Arc::new(data_node);
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
        for data_node in self.nodes.values() {
            count += data_node.has_volumes().await?;
        }
        Ok(count)
    }

    pub async fn max_volumes(&self) -> Result<i64> {
        let mut max_volumes = 0;
        for data_node in self.nodes.values() {
            max_volumes += data_node.max_volumes;
        }
        Ok(max_volumes)
    }

    pub async fn free_volumes(&self) -> Result<i64> {
        let mut free_volumes = 0;
        for data_node in self.nodes.values() {
            free_volumes += data_node.free_volumes().await?;
        }
        Ok(free_volumes)
    }

    pub async fn reserve_one_volume(&self) -> Result<Arc<DataNode>> {
        // randomly select
        let mut free_volumes = 0;
        for (_, data_node) in self.nodes.iter() {
            free_volumes += data_node.free_volumes().await?;
        }

        let idx = random::<u32>() as i64 % free_volumes;

        for (_, data_node) in self.nodes.iter() {
            free_volumes -= data_node.free_volumes().await?;
            if free_volumes == idx {
                return Ok(data_node.clone());
            }
        }

        Err(Error::NoFreeSpace(format!(
            "reserve one volume on rack {} fail",
            self.id
        )))
    }
}
