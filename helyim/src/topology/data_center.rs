use std::{result::Result as StdResult, sync::Arc};

use dashmap::DashMap;
use faststr::FastStr;
use serde::Serialize;

use crate::{
    storage::{VolumeError, VolumeId},
    topology::{
        node::{downcast_rack, Node, NodeImpl, NodeType},
        rack::{Rack, RackRef},
        DataNodeRef,
    },
};

#[derive(Serialize)]
pub struct DataCenter {
    node: Arc<NodeImpl>,
}

impl DataCenter {
    pub fn new(id: FastStr) -> DataCenter {
        let node = Arc::new(NodeImpl::new(id));
        Self { node }
    }

    pub async fn get_or_create_rack(&self, id: &str) -> Result<RackRef, VolumeError> {
        match self.children().get(id) {
            Some(rack) => downcast_rack(rack.clone()),
            None => {
                let rack = Arc::new(Rack::new(FastStr::new(id)));
                self.link_rack(rack.clone()).await;
                Ok(rack)
            }
        }
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
