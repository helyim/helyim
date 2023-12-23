use std::collections::HashMap;

use faststr::FastStr;
use serde::Serialize;

use crate::{
    storage::{ReplicaPlacement, Ttl, VolumeId},
    topology::{volume_layout::VolumeLayout, DataNodeRef},
};

#[derive(Clone, Serialize)]
pub struct Collection {
    name: FastStr,
    volume_size_limit: u64,
    #[serde(skip)]
    pub volume_layouts: HashMap<FastStr, VolumeLayout>,
}

impl Collection {
    pub fn new(name: FastStr, volume_size_limit: u64) -> Collection {
        Collection {
            name,
            volume_size_limit,
            volume_layouts: HashMap::new(),
        }
    }

    pub fn get_or_create_volume_layout(
        &mut self,
        rp: ReplicaPlacement,
        ttl: Option<Ttl>,
    ) -> &mut VolumeLayout {
        let key = match ttl {
            Some(ttl) => format!("{}{}", rp, ttl),
            None => rp.to_string(),
        };

        let volume_size = self.volume_size_limit;

        self.volume_layouts
            .entry(FastStr::from_string(key))
            .or_insert_with(|| VolumeLayout::new(rp, ttl, volume_size))
    }

    pub async fn lookup(&self, vid: VolumeId) -> Option<Vec<DataNodeRef>> {
        for layout in self.volume_layouts.values() {
            let ret = layout.lookup(vid);
            if ret.is_some() {
                return ret;
            }
        }

        None
    }
}
