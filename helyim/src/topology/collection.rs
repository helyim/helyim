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
