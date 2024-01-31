use std::sync::Arc;

use dashmap::{mapref::one::RefMut, DashMap};
use rand::{self, Rng};
use serde::Serialize;
use tokio::sync::RwLock;

use crate::{
    storage::{ReplicaPlacement, Ttl, VolumeError, VolumeId, VolumeInfo, CURRENT_VERSION},
    topology::{data_node::DataNodeRef, volume_grow::VolumeGrowOption},
};

#[derive(Serialize)]
pub struct VolumeLayout {
    rp: ReplicaPlacement,
    ttl: Option<Ttl>,
    volume_size_limit: u64,

    #[serde(skip)]
    writable_volumes: RwLock<Vec<VolumeId>>,
    pub readonly_volumes: DashMap<VolumeId, bool>,
    oversize_volumes: DashMap<VolumeId, bool>,
    #[serde(skip)]
    pub locations: DashMap<VolumeId, Vec<DataNodeRef>>,
}

impl VolumeLayout {
    pub fn new(rp: ReplicaPlacement, ttl: Option<Ttl>, volume_size_limit: u64) -> VolumeLayout {
        VolumeLayout {
            rp,
            ttl,
            volume_size_limit,
            writable_volumes: RwLock::new(Vec::new()),
            readonly_volumes: DashMap::new(),
            oversize_volumes: DashMap::new(),
            locations: DashMap::new(),
        }
    }

    pub async fn active_volume_count(&self, option: &VolumeGrowOption) -> i64 {
        if option.data_center.is_empty() {
            return self.writable_volumes.read().await.len() as i64;
        }
        let mut count = 0;

        for vid in self.writable_volumes.read().await.iter() {
            if let Some(nodes) = self.locations.get(vid) {
                for node in nodes.iter() {
                    if node.data_center_id().await == option.data_center {
                        if !option.rack.is_empty() && node.rack_id().await != option.rack {
                            continue;
                        }
                        if !option.data_node.is_empty() && node.id() != option.data_node {
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
    ) -> Result<(VolumeId, Vec<DataNodeRef>), VolumeError> {
        if self.writable_volumes.read().await.is_empty() {
            return Err(VolumeError::NoWritableVolumes);
        }

        if option.data_center.is_empty() {
            let len = self.writable_volumes.read().await.len();
            let vid = self.writable_volumes.read().await[rand::thread_rng().gen_range(0..len)];
            return match self.locations.get(&vid) {
                Some(data_nodes) => Ok((vid, data_nodes.value().clone())),
                None => Err(VolumeError::NotFound(vid)),
            };
        }

        let mut counter = 0;

        let mut volume_id = 0;
        let mut location_list = None;

        for vid in self.writable_volumes.read().await.iter() {
            if let Some(locations) = self.locations.get(vid) {
                for node in locations.iter() {
                    if node.data_center_id().await == option.data_center {
                        if !option.rack.is_empty() && node.rack_id().await != option.rack {
                            continue;
                        }
                        if !option.data_node.is_empty() && node.id() != option.data_node {
                            continue;
                        }

                        counter += 1;
                        if rand::thread_rng().gen_range(0..counter) < 1 {
                            volume_id = *vid;
                            location_list = Some(locations.value().clone());
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

    async fn set_node(locations: &mut RefMut<'_, VolumeId, Vec<DataNodeRef>>, dn: DataNodeRef) {
        for location in locations.iter_mut() {
            if location.ip == dn.ip && location.port == dn.port {
                *location = dn.clone();
                return;
            }
        }
        locations.push(dn);
    }

    pub async fn register_volume(&self, v: &VolumeInfo, dn: &DataNodeRef) {
        // release write lock
        {
            let mut locations = self.locations.entry(v.id).or_default();
            VolumeLayout::set_node(&mut locations, dn.clone()).await;
        }

        let locations = self.locations.get(&v.id).unwrap();
        for location in locations.iter() {
            let volume = location.get_volume(v.id);
            match volume {
                Some(v) => {
                    if v.read_only {
                        self.remove_from_writable(&v.id).await;
                        self.readonly_volumes.insert(v.id, true);
                        return;
                    } else {
                        self.readonly_volumes.remove(&v.id);
                    }
                }
                None => {
                    self.remove_from_writable(&v.id).await;
                    self.readonly_volumes.remove(&v.id);
                    return;
                }
            }
        }

        self.remember_oversized_volume(v);
        self.ensure_correct_writable(v).await;
    }

    async fn ensure_correct_writable(&self, v: &VolumeInfo) {
        if let Some(locations) = self.locations.get(&v.id) {
            if locations.len() >= self.rp.copy_count() && self.is_writable(v) {
                if !self.oversize_volumes.contains_key(&v.id) {
                    self.set_volume_writable(v.id).await;
                }
            } else {
                self.remove_from_writable(&v.id).await;
            }
        }
    }

    fn remember_oversized_volume(&self, v: &VolumeInfo) {
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

    pub async fn set_volume_available(&self, vid: VolumeId, data_node: &DataNodeRef) -> bool {
        if let Some(mut locations) = self.locations.get_mut(&vid) {
            VolumeLayout::set_node(&mut locations, data_node.clone()).await;

            if locations.len() == self.rp.copy_count() {
                return self.set_volume_writable(vid).await;
            }
        }
        false
    }

    pub async fn set_volume_unavailable(&self, vid: &VolumeId, data_node: &DataNodeRef) -> bool {
        if let Some(mut locations) = self.locations.get_mut(vid) {
            locations
                .value_mut()
                .retain(|node| node.ip != data_node.ip || node.port != data_node.port);
            if locations.len() < self.rp.copy_count() {
                return self.remove_from_writable(vid).await;
            }
        }
        false
    }

    pub async fn set_volume_writable(&self, vid: VolumeId) -> bool {
        if self.writable_volumes.read().await.contains(&vid) {
            return false;
        }
        self.writable_volumes.write().await.push(vid);
        true
    }

    pub async fn remove_from_writable(&self, vid: &VolumeId) -> bool {
        let mut idx = -1;
        for (i, id) in self.writable_volumes.read().await.iter().enumerate() {
            if id == vid {
                idx = i as i32;
            }
        }
        if idx >= 0 {
            self.writable_volumes.write().await.remove(idx as usize);
            return true;
        }
        false
    }

    pub async fn unregister_volume(&self, v: &VolumeInfo, data_node: &DataNodeRef) {
        if let Some(mut location) = self.locations.get_mut(&v.id) {
            let mut removed = true;
            location.value_mut().retain(|node| {
                if node.ip != data_node.ip || node.port != data_node.port {
                    true
                } else {
                    removed = true;
                    false
                }
            });
            if removed {
                self.ensure_correct_writable(v).await;
            }
        }
        self.locations.retain(|_, locations| !locations.is_empty());
    }

    pub fn lookup(&self, vid: VolumeId) -> Option<Vec<DataNodeRef>> {
        self.locations.get(&vid).map(|value| value.value().clone())
    }
}

pub type VolumeLayoutRef = Arc<VolumeLayout>;
