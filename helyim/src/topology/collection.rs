use std::sync::Arc;

use async_lock::RwLock;
use dashmap::{mapref::one::RefMut, DashMap};
use faststr::FastStr;
use serde::Serialize;

use crate::{
    storage::{ReplicaPlacement, Ttl, VolumeId},
    topology::{volume_layout::VolumeLayout, DataNode},
};

#[derive(Clone, Debug, Serialize)]
pub struct Collection {
    name: FastStr,
    volume_size_limit: u64,
    pub volume_layouts: DashMap<FastStr, VolumeLayout>,
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
    ) -> RefMut<FastStr, VolumeLayout> {
        let key = match ttl {
            Some(ttl) => format!("{}{}", rp, ttl),
            None => rp.to_string(),
        };

        let volume_size = self.volume_size_limit;

        self.volume_layouts
            .entry(FastStr::from_string(key))
            .or_insert_with(|| VolumeLayout::new(rp, ttl, volume_size))
    }

    pub fn lookup(&self, vid: VolumeId) -> Option<Vec<Arc<RwLock<DataNode>>>> {
        for layout in self.volume_layouts.iter() {
            let ret = layout.lookup(vid);
            if ret.is_some() {
                return ret;
            }
        }

        None
    }
}
