use std::sync::Arc;

use dashmap::DashMap;
use faststr::FastStr;
use helyim_common::{ttl::Ttl, types::VolumeId};
use serde::Serialize;

use crate::{
    storage::ReplicaPlacement,
    topology::{
        volume_layout::{VolumeLayout, VolumeLayoutRef},
        DataNodeRef,
    },
};

#[derive(Clone, Serialize)]
pub struct Collection {
    name: FastStr,
    volume_size_limit: u64,
    #[serde(skip)]
    pub volume_layouts: DashMap<FastStr, VolumeLayoutRef>,
}

impl Collection {
    pub fn new(name: FastStr, volume_size_limit: u64) -> Collection {
        Collection {
            name,
            volume_size_limit,
            volume_layouts: DashMap::new(),
        }
    }

    pub fn get_or_create_volume_layout(
        &self,
        rp: ReplicaPlacement,
        ttl: Option<Ttl>,
    ) -> VolumeLayoutRef {
        let key = match ttl {
            Some(ttl) => format!("{}{}", rp, ttl),
            None => rp.to_string(),
        };

        match self.volume_layouts.get(key.as_str()) {
            Some(vl) => vl.value().clone(),
            None => {
                let volume_layout = Arc::new(VolumeLayout::new(rp, ttl, self.volume_size_limit));
                self.volume_layouts
                    .insert(FastStr::new(key), volume_layout.clone());
                volume_layout
            }
        }
    }

    pub async fn lookup(&self, vid: VolumeId) -> Option<Vec<DataNodeRef>> {
        for layout in self.volume_layouts.iter() {
            let ret = layout.lookup(vid);
            if ret.is_some() {
                return ret;
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use faststr::FastStr;
    use helyim_common::ttl::Ttl;

    use crate::{
        storage::{ReplicaPlacement, VolumeInfo, CURRENT_VERSION},
        topology::{collection::Collection, data_node::DataNode},
    };

    fn data_node() -> DataNode {
        let id = FastStr::new("127.0.0.1:8080");
        let ip = FastStr::new("127.0.0.1");
        let public_url = id.clone();
        let data_node = DataNode::new(id, ip, 8080, public_url, 1);
        data_node.volumes.insert(0, VolumeInfo::default());
        data_node
    }

    #[test]
    pub fn test_get_or_create_volume_layout() {
        let collection = Collection::new(FastStr::new("default"), 1024);

        let rp = ReplicaPlacement::new("000").unwrap();
        let ttl = Ttl::new("1d").unwrap();
        let vl = collection.get_or_create_volume_layout(rp, Some(ttl));
        let _vl1 = collection.get_or_create_volume_layout(rp, Some(ttl));

        assert_eq!(Arc::strong_count(&vl), 3);

        let vl = collection.get_or_create_volume_layout(rp, None);
        let _vl1 = collection.get_or_create_volume_layout(rp, None);

        assert_eq!(Arc::strong_count(&vl), 3);
    }

    #[tokio::test]
    pub async fn test_lookup() {
        let collection = Collection::new(FastStr::new("default"), 1024);

        let rp = ReplicaPlacement::new("000").unwrap();
        let ttl = Ttl::new("1d").unwrap();
        let vl = collection.get_or_create_volume_layout(rp, Some(ttl));

        let volume_opt = collection.lookup(0).await;
        let volume1_opt = vl.lookup(0);

        assert!(volume_opt.is_none());
        assert!(volume1_opt.is_none());

        let data_node = Arc::new(data_node());
        let volume_info = VolumeInfo {
            version: CURRENT_VERSION,
            ..Default::default()
        };

        vl.register_volume(&volume_info, &data_node).await;

        let volume_opt = collection.lookup(0).await;
        let volume1_opt = vl.lookup(0);

        assert!(volume_opt.is_some());
        assert!(volume1_opt.is_some());
    }
}
