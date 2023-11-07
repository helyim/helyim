use std::{collections::HashMap, sync::Arc};

use anyhow::anyhow;
use rand::{self, Rng};
use serde::Serialize;

use crate::{
    errors::Result,
    storage::{ReplicaPlacement, Ttl, VolumeError, VolumeId, VolumeInfo, CURRENT_VERSION},
    topology::{volume_grow::VolumeGrowOption, DataNode},
};

#[derive(Clone, Debug, Serialize)]
pub struct VolumeLayout {
    rp: ReplicaPlacement,
    ttl: Option<Ttl>,
    volume_size_limit: u64,

    writable_volumes: Vec<VolumeId>,
    pub readonly_volumes: HashMap<VolumeId, bool>,
    oversize_volumes: HashMap<VolumeId, bool>,
    #[serde(skip)]
    pub locations: HashMap<VolumeId, Vec<Arc<DataNode>>>,
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

    pub async fn active_volume_count(&self, option: Arc<VolumeGrowOption>) -> Result<i64> {
        if option.data_center.is_empty() {
            return Ok(self.writable_volumes.len() as i64);
        }
        let mut count = 0;

        for vid in self.writable_volumes.iter() {
            if let Some(nodes) = self.locations.get(vid) {
                for node in nodes.iter() {
                    if node.data_center_id().await? == option.data_center {
                        if !option.rack.is_empty() && node.rack_id().await? != option.rack {
                            continue;
                        }
                        if !option.data_node.is_empty() && node.id != option.data_node {
                            continue;
                        }
                        count += 1;
                    }
                }
            }
        }

        Ok(count)
    }

    pub async fn pick_for_write(
        &self,
        option: &VolumeGrowOption,
    ) -> Result<(VolumeId, &Vec<Arc<DataNode>>)> {
        if self.writable_volumes.is_empty() {
            return Err(VolumeError::NoWritableVolumes.into());
        }

        if option.data_center.is_empty() {
            let len = self.writable_volumes.len();
            let vid = self.writable_volumes[rand::thread_rng().gen_range(0..len)];
            return match self.locations.get(&vid) {
                Some(data_nodes) => Ok((vid, data_nodes)),
                None => Err(anyhow!("Strangely vid {} is on no machine!", vid).into()),
            };
        }

        let mut counter = 0;

        let mut volume_id = 0;
        let mut location_list = None;

        for vid in self.writable_volumes.iter() {
            if let Some(locations) = self.locations.get(vid) {
                for node in locations.iter() {
                    if node.data_center_id().await? == option.data_center {
                        if !option.rack.is_empty() && node.rack_id().await? != option.rack {
                            continue;
                        }
                        if !option.data_node.is_empty() && node.id != option.data_node {
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
            None => Err(VolumeError::NoWritableVolumes.into()),
        }
    }

    fn set_node(locations: &mut Vec<Arc<DataNode>>, dn: Arc<DataNode>) -> Result<()> {
        for location in locations.iter_mut() {
            if location.ip == dn.ip && location.port == dn.port {
                *location = dn.clone();
                return Ok(());
            }
        }
        locations.push(dn);
        Ok(())
    }

    pub async fn register_volume(&mut self, v: &VolumeInfo, dn: Arc<DataNode>) -> Result<()> {
        let locations = self.locations.entry(v.id).or_default();
        VolumeLayout::set_node(locations, dn)?;

        for location in locations.iter() {
            match location.get_volume(v.id).await {
                Ok(Some(v)) => {
                    if v.read_only {
                        self.remove_from_writable(v.id);
                        self.readonly_volumes.insert(v.id, true);
                        return Ok(());
                    }
                }
                Ok(None) => {
                    self.readonly_volumes.remove(&v.id);
                }
                Err(_) => {
                    self.remove_from_writable(v.id);
                    self.readonly_volumes.remove(&v.id);
                    return Ok(());
                }
            }
        }

        self.set_oversize_if_need(v);
        self.ensure_correct_writable(v);

        Ok(())
    }

    fn ensure_correct_writable(&mut self, v: &VolumeInfo) {
        if let Some(locations) = self.locations.get(&v.id) {
            if locations.len() == self.rp.copy_count() && self.is_writable(v) {
                if !self.oversize_volumes.contains_key(&v.id) {
                    self.add_to_writable(v.id);
                }
            } else {
                self.remove_from_writable(v.id);
            }
        }
    }

    fn set_oversize_if_need(&mut self, v: &VolumeInfo) {
        if self.is_oversize(v) {
            self.oversize_volumes.insert(v.id, true);
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
        data_node: &Arc<DataNode>,
    ) -> Result<()> {
        if let Some(locations) = self.locations.get_mut(&vid) {
            let mut should_add = true;
            for location in locations.iter_mut() {
                if data_node.ip == location.ip && data_node.port == location.port {
                    should_add = false;
                }
            }
            if should_add {
                locations.push(data_node.clone());
            }
            if locations.len() >= self.rp.copy_count() {
                self.add_to_writable(vid);
            }
        }

        Ok(())
    }

    pub fn add_to_writable(&mut self, vid: VolumeId) {
        if !self.writable_volumes.contains(&vid) {
            self.writable_volumes.push(vid);
        }
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

    pub fn lookup(&self, vid: VolumeId) -> Option<Vec<Arc<DataNode>>> {
        self.locations.get(&vid).cloned()
    }
}
