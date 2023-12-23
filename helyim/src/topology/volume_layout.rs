use std::{collections::HashMap, sync::Arc};

use rand::{self, Rng};
use serde::Serialize;

use crate::{
    storage::{ReplicaPlacement, Ttl, VolumeError, VolumeId, VolumeInfo, CURRENT_VERSION},
    topology::{data_node::DataNodeRef, volume_grow::VolumeGrowOption},
};

#[derive(Clone, Serialize)]
pub struct VolumeLayout {
    rp: ReplicaPlacement,
    ttl: Option<Ttl>,
    volume_size_limit: u64,

    writable_volumes: Vec<VolumeId>,
    pub readonly_volumes: HashMap<VolumeId, bool>,
    oversize_volumes: HashMap<VolumeId, bool>,
    #[serde(skip)]
    pub locations: HashMap<VolumeId, Vec<DataNodeRef>>,
}

impl VolumeLayout {
    pub fn new(rp: ReplicaPlacement, ttl: Option<Ttl>, volume_size_limit: u64) -> VolumeLayout {
        VolumeLayout {
            rp,
            ttl,
            volume_size_limit,
            writable_volumes: Vec::new(),
            readonly_volumes: HashMap::new(),
            oversize_volumes: HashMap::new(),
            locations: HashMap::new(),
        }
    }

    pub async fn active_volume_count(&self, option: Arc<VolumeGrowOption>) -> i64 {
        if option.data_center.is_empty() {
            return self.writable_volumes.len() as i64;
        }
        let mut count = 0;

        for vid in self.writable_volumes.iter() {
            if let Some(nodes) = self.locations.get(vid) {
                for node in nodes.iter() {
                    if node.read().await.data_center_id().await == option.data_center {
                        if !option.rack.is_empty()
                            && node.read().await.rack_id().await != option.rack
                        {
                            continue;
                        }
                        if !option.data_node.is_empty()
                            && node.read().await.id() != option.data_node
                        {
                            continue;
                        }
                        count += 1;
                    }
                }
            }
        }

        count
    }

    pub async fn pick_for_write(
        &self,
        option: &VolumeGrowOption,
    ) -> Result<(VolumeId, &Vec<DataNodeRef>), VolumeError> {
        if self.writable_volumes.is_empty() {
            return Err(VolumeError::NoWritableVolumes);
        }

        if option.data_center.is_empty() {
            let len = self.writable_volumes.len();
            let vid = self.writable_volumes[rand::thread_rng().gen_range(0..len)];
            return match self.locations.get(&vid) {
                Some(data_nodes) => Ok((vid, data_nodes)),
                None => Err(VolumeError::NotFound(vid)),
            };
        }

        let mut counter = 0;

        let mut volume_id = 0;
        let mut location_list = None;

        for vid in self.writable_volumes.iter() {
            if let Some(locations) = self.locations.get(vid) {
                for node in locations.iter() {
                    if node.read().await.data_center_id().await == option.data_center {
                        if !option.rack.is_empty()
                            && node.read().await.rack_id().await != option.rack
                        {
                            continue;
                        }
                        if !option.data_node.is_empty()
                            && node.read().await.id() != option.data_node
                        {
                            continue;
                        }

                        counter += 1;
                        if rand::thread_rng().gen_range(0..counter) < 1 {
                            volume_id = *vid;
                            location_list = Some(locations);
                        }
                    }
                }
            }
        }

        match location_list {
            Some(locations) => Ok((volume_id, locations)),
            None => Err(VolumeError::NoWritableVolumes),
        }
    }

    async fn set_node(locations: &mut Vec<DataNodeRef>, dn: DataNodeRef) {
        for location in locations.iter_mut() {
            if location.read().await.ip == dn.read().await.ip
                && location.read().await.port == dn.read().await.port
            {
                *location = dn.clone();
                return;
            }
        }
        locations.push(dn);
    }

    pub async fn register_volume(&mut self, v: &VolumeInfo, dn: DataNodeRef) {
        let locations = self.locations.entry(v.id).or_default();
        VolumeLayout::set_node(locations, dn).await;

        let mut removed = vec![];
        for location in locations.iter() {
            match location.read().await.get_volume(v.id) {
                Some(v) => {
                    if v.read_only {
                        removed.push(v.id);
                        self.readonly_volumes.insert(v.id, true);
                        return;
                    } else {
                        self.readonly_volumes.remove(&v.id);
                    }
                }
                None => {
                    removed.push(v.id);
                    self.readonly_volumes.remove(&v.id);
                    return;
                }
            }
        }

        for vid in removed {
            self.remove_from_writable(vid);
        }

        self.remember_oversized_volume(v);
        self.ensure_correct_writable(v);
    }

    fn ensure_correct_writable(&mut self, v: &VolumeInfo) {
        if let Some(locations) = self.locations.get(&v.id) {
            if locations.len() >= self.rp.copy_count() && self.is_writable(v) {
                if !self.oversize_volumes.contains_key(&v.id) {
                    self.set_volume_writable(v.id);
                }
            } else {
                self.remove_from_writable(v.id);
            }
        }
    }

    fn remember_oversized_volume(&mut self, v: &VolumeInfo) {
        if self.is_oversize(v) {
            self.oversize_volumes.insert(v.id, true);
        } else {
            self.oversize_volumes.remove(&v.id);
        }
    }

    fn is_oversize(&self, v: &VolumeInfo) -> bool {
        v.size >= self.volume_size_limit
    }

    fn is_writable(&self, v: &VolumeInfo) -> bool {
        !self.is_oversize(v) && v.version == CURRENT_VERSION && !v.read_only
    }

    pub async fn set_volume_available(
        &mut self,
        vid: VolumeId,
        data_node: &DataNodeRef,
        readonly: bool,
    ) -> bool {
        if let Some(volume_info) = data_node.read().await.get_volume(vid) {
            if let Some(locations) = self.locations.get_mut(&vid) {
                VolumeLayout::set_node(locations, data_node.clone()).await;
            }

            if volume_info.read_only || readonly {
                return false;
            }

            if let Some(locations) = self.locations.get_mut(&vid) {
                if locations.len() >= self.rp.copy_count() {
                    return self.set_volume_writable(vid);
                }
            }
        }
        false
    }

    pub fn set_volume_writable(&mut self, vid: VolumeId) -> bool {
        if self.writable_volumes.contains(&vid) {
            return false;
        }
        self.writable_volumes.push(vid);
        true
    }

    pub fn remove_from_writable(&mut self, vid: VolumeId) {
        let mut idx = -1;
        for (i, id) in self.writable_volumes.iter().enumerate() {
            if *id == vid {
                idx = i as i32;
            }
        }
        if idx > 0 {
            self.writable_volumes.remove(idx as usize);
        }
    }

    pub fn unregister_volume(&mut self, v: &VolumeInfo) {
        self.remove_from_writable(v.id);
    }

    pub fn lookup(&self, vid: VolumeId) -> Option<Vec<DataNodeRef>> {
        self.locations.get(&vid).cloned()
    }
}
