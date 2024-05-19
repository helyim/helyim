use std::{
    result::Result as StdResult,
    sync::{Arc, Weak},
};

use dashmap::DashMap;
use faststr::FastStr;
use serde::Serialize;
use tokio::sync::RwLock;

use crate::{
    storage::{VolumeError, VolumeId},
    topology::{
        node::{Node, NodeImpl, NodeType},
        rack::{Rack, RackRef},
        DataNodeRef, Topology,
    },
};

#[derive(Serialize)]
pub struct DataCenter {
    node: Arc<NodeImpl>,
    // parent
    #[serde(skip)]
    pub topology: RwLock<Weak<Topology>>,
    // children
    #[serde(skip)]
    pub racks: DashMap<FastStr, RackRef>,
}

impl DataCenter {
    pub fn new(id: FastStr) -> DataCenter {
        let node = Arc::new(NodeImpl::new(id));
        Self {
            node,
            topology: RwLock::new(Weak::new()),
            racks: DashMap::new(),
        }
    }

    pub async fn set_topology(&self, topology: Weak<Topology>) {
        *self.topology.write().await = topology;
    }

    pub async fn get_or_create_rack(&self, id: &str) -> RackRef {
        match self.racks.get(id) {
            Some(rack) => rack.value().clone(),
            None => {
                let rack = Arc::new(Rack::new(FastStr::new(id)));
                self.link_rack(rack.clone()).await;
                rack
            }
        }
    }

    pub async fn reserve_one_volume(&self, mut random: i64) -> StdResult<DataNodeRef, VolumeError> {
        for rack in self.racks.iter() {
            let free_space = rack.free_space();
            if free_space <= 0 {
                continue;
            }
            if random >= free_space {
                random -= free_space;
            } else {
                let node = rack.reserve_one_volume(random).await?;
                return Ok(node);
            }
        }

        Err(VolumeError::NoFreeSpace(format!(
            "no free volumes found on data center {}",
            self.id()
        )))
    }
}

impl DataCenter {
    pub async fn link_rack(&self, rack: Arc<Rack>) {
        let data_center_node = self.node.clone();
        data_center_node.link_child_node(rack).await;
    }
}

impl_node!(DataCenter);

pub type DataCenterRef = Arc<DataCenter>;
