use std::sync::Arc;

use async_lock::RwLock;
use dashmap::{DashMap, DashSet};
use rand;
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

    #[serde(skip)]
    writable_volumes: Arc<DashSet<VolumeId>>,
    #[serde(skip)]
    pub readonly_volumes: Arc<DashSet<VolumeId>>,
    #[serde(skip)]
    oversize_volumes: Arc<DashSet<VolumeId>>,
    #[serde(skip)]
    pub locations: Arc<DashMap<VolumeId, Vec<Arc<RwLock<DataNode>>>>>,
}

impl VolumeLayout {
    pub fn new(rp: ReplicaPlacement, ttl: Option<Ttl>, volume_size_limit: u64) -> VolumeLayout {
        VolumeLayout {
            rp,
            ttl,
            volume_size_limit,
            writable_volumes: Arc::new(DashSet::new()),
            readonly_volumes: Arc::new(DashSet::new()),
            oversize_volumes: Arc::new(DashSet::new()),
            locations: Arc::new(DashMap::new()),
        }
    }

    pub async fn active_volume_count(&self, option: VolumeGrowOption) -> Result<i64> {
        if option.data_center.is_empty() {
            return Ok(self.writable_volumes.len() as i64);
        }
        let mut count = 0;

        for vid in self.writable_volumes.iter() {
            if let Some(nodes) = self.locations.get(vid.key()) {
                for node in nodes.value() {
                    if node.read().await.id == option.data_node
                        && node.read().await.rack_id().await == option.rack
                        && node.read().await.data_center_id().await == option.data_center
                    {
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
    ) -> Result<(VolumeId, Vec<Arc<RwLock<DataNode>>>)> {
        if self.writable_volumes.is_empty() {
            return Err(VolumeError::NoWritableVolumes.into());
        }

        let mut counter = 0;
        let mut ret = (0, vec![]);

        for vid in self.writable_volumes.iter() {
            if let Some(locations) = self.locations.get(vid.key()) {
                for node in locations.iter() {
                    if !option.data_center.is_empty()
                        && option.data_center != node.read().await.data_center_id().await
                        || !option.rack.is_empty()
                            && option.rack != node.read().await.rack_id().await
                        || !option.data_node.is_empty() && option.data_node != node.read().await.id
                    {
                        continue;
                    }

                    counter += 1;
                    if rand::random::<i64>() % counter < 1 {
                        ret = (*vid, locations.clone());
                    }
                }
            }
        }

        if counter > 0 {
            return Ok(ret);
        }

        Err(VolumeError::NoWritableVolumes.into())
    }

    async fn set_node(
        locations: &mut Vec<Arc<RwLock<DataNode>>>,
        dn: Arc<RwLock<DataNode>>,
    ) -> Result<()> {
        let mut same: Option<usize> = None;
        let mut i = 0;
        for location in locations.iter() {
            if location.read().await.ip != dn.read().await.ip
                || location.read().await.port != dn.read().await.port
            {
                i += 1;
                continue;
            }

            same = Some(i);
            break;
        }
        if let Some(idx) = same {
            locations[idx] = dn.clone();
        } else {
            locations.push(dn.clone())
        }

        Ok(())
    }

    pub async fn register_volume(
        &mut self,
        v: &VolumeInfo,
        dn: Arc<RwLock<DataNode>>,
    ) -> Result<()> {
        {
            let mut list = self.locations.entry(v.id).or_default();
            VolumeLayout::set_node(list.value_mut(), dn).await?;
        }

        let mut locations = vec![];
        if let Some(list) = self.locations.get(&v.id) {
            locations.extend_from_slice(list.value());
        }

        for location in locations.iter() {
            match location.read().await.get_volume(v.id) {
                Some(v) => {
                    if v.read_only {
                        self.remove_from_writable(v.id);
                        self.readonly_volumes.insert(v.id);
                    }
                }
                None => {
                    self.remove_from_writable(v.id);
                    self.readonly_volumes.remove(&v.id);
                }
            }
        }

        if locations.len() == self.rp.copy_count() && self.is_writable(v) {
            if self.oversize_volumes.get(&v.id).is_none() {
                self.add_to_writable(v.id);
            }
        } else {
            self.remove_from_writable(v.id);
            self.set_oversize_if_need(v);
        }

        Ok(())
    }

    fn set_oversize_if_need(&mut self, v: &VolumeInfo) {
        if self.is_oversize(v) {
            self.oversize_volumes.insert(v.id);
        }
    }

    fn is_oversize(&self, v: &VolumeInfo) -> bool {
        v.size >= self.volume_size_limit
    }

    fn is_writable(&self, v: &VolumeInfo) -> bool {
        !self.is_oversize(v) && v.version == CURRENT_VERSION && !v.read_only
    }

    pub async fn set_volume_available(
        &self,
        vid: VolumeId,
        data_node: &Arc<RwLock<DataNode>>,
    ) -> Result<()> {
        if let Some(mut entry) = self.locations.get_mut(&vid) {
            let mut should_add = true;
            for location in entry.value() {
                if data_node.read().await.ip == location.read().await.ip
                    && data_node.read().await.port == location.read().await.port
                {
                    should_add = false;
                }
            }
            if should_add {
                entry.value_mut().push(data_node.clone());
            }
            if entry.value().len() >= self.rp.copy_count() {
                self.add_to_writable(vid);
            }
        }

        Ok(())
    }

    pub fn add_to_writable(&self, vid: VolumeId) {
        self.writable_volumes.insert(vid);
    }

    pub fn remove_from_writable(&self, vid: VolumeId) {
        self.writable_volumes.remove(&vid);
    }

    pub fn unregister_volume(&mut self, v: &VolumeInfo) {
        self.remove_from_writable(v.id);
    }

    pub fn lookup(&self, vid: VolumeId) -> Option<Vec<Arc<RwLock<DataNode>>>> {
        self.locations
            .get(&vid)
            .map(|location| location.value().clone())
    }
}
