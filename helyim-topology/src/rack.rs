use std::{result::Result as StdResult, sync::Arc};

use dashmap::DashMap;
use faststr::FastStr;
use helyim_common::types::VolumeId;
use serde::Serialize;

use crate::{
    DataNodeRef,
    data_node::DataNode,
    node::{Node, NodeImpl, downcast_node},
};

#[derive(Serialize)]
pub struct Rack {
    node: Arc<NodeImpl>,
}

impl Rack {
    pub fn new(id: FastStr) -> Rack {
        let node = Arc::new(NodeImpl::new(id));
        Self { node }
    }

    pub async fn get_or_create_data_node(
        &self,
        id: FastStr,
        ip: FastStr,
        port: u16,
        public_url: FastStr,
        max_volume_count: i64,
    ) -> DataNodeRef {
        match self.children().get(&id) {
            Some(data_node) => downcast_node(data_node.clone()).unwrap(),
            None => {
                let data_node = Arc::new(DataNode::new(
                    id.clone(),
                    ip,
                    port,
                    public_url,
                    max_volume_count,
                ));
                self.link_data_node(data_node.clone()).await;
                data_node
            }
        }
    }

    pub async fn data_center_id(&self) -> FastStr {
        match self.node.parent().await {
            Some(data_center) => FastStr::new(data_center.id()),
            None => FastStr::empty(),
        }
    }
}

impl Rack {
    pub async fn link_data_node(&self, data_node: Arc<DataNode>) {
        let rack_node = self.node.clone();
        rack_node.link_child_node(data_node).await;
    }
}

impl_node!(Rack);

pub type RackRef = Arc<Rack>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use faststr::FastStr;
    use rand::Rng;

    use crate::{node::Node, rack::Rack};

    #[tokio::test]
    pub async fn test_get_or_create_data_node() {
        let rack = Rack::new(FastStr::new("default"));

        let id = FastStr::new("127.0.0.1:8080");
        let node = rack
            .get_or_create_data_node(id.clone(), FastStr::new("127.0.0.1"), 8080, id.clone(), 1)
            .await;

        let _node1 = rack
            .get_or_create_data_node(id.clone(), FastStr::new("127.0.0.1"), 8080, id, 1)
            .await;

        assert_eq!(Arc::strong_count(&node), 3);
    }

    #[tokio::test]
    pub async fn test_reserve_one_volume() {
        let rack = Rack::new(FastStr::new("default"));

        let id = FastStr::new("127.0.0.1:8080");
        let node = rack
            .get_or_create_data_node(id.clone(), FastStr::new("127.0.0.1"), 8080, id.clone(), 1)
            .await;

        let random = rand::thread_rng().gen_range(0..rack.free_space());
        let _node1 = rack.reserve_one_volume(random).unwrap();

        assert_eq!(Arc::strong_count(&node), 3);
    }
}
