use std::sync::Arc;

use dashmap::DashMap;
use faststr::FastStr;
use serde::Serialize;

use crate::{
    storage::{ReplicaPlacement, Ttl, VolumeId},
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

        let volume_size = self.volume_size_limit;

        if !self.volume_layouts.contains_key(key.as_str()) {
            let volume_layout = Arc::new(VolumeLayout::new(rp, ttl, volume_size));
            self.volume_layouts
                .insert(FastStr::new(key.as_str()), volume_layout);
        }

        let volume_layout = self.volume_layouts.get(key.as_str()).unwrap();
        volume_layout.value().clone()
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
