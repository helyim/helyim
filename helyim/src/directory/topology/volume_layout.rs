use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use futures::lock::Mutex;
use rand;
use serde::Serialize;

use crate::{
    directory::topology::{volume_grow::VolumeGrowOption, DataNode, DataNodeEventTx},
    errors::{Error, Result},
    storage::{ReplicaPlacement, Ttl, VolumeId, VolumeInfo, CURRENT_VERSION},
};

#[derive(Clone, Debug, Serialize)]
pub struct VolumeLayout {
    pub rp: ReplicaPlacement,
    pub ttl: Option<Ttl>,
    pub volume_size_limit: u64,

    pub writable_volumes: Vec<VolumeId>,
    pub readonly_volumes: HashSet<VolumeId>,
    pub oversize_volumes: HashSet<VolumeId>,
    #[serde(skip)]
    pub locations: HashMap<VolumeId, Vec<Arc<Mutex<DataNode>>>>,
    #[serde(skip)]
    pub locations_tx: HashMap<VolumeId, Vec<DataNodeEventTx>>,
}

impl VolumeLayout {
    pub fn new(rp: ReplicaPlacement, ttl: Option<Ttl>, volume_size_limit: u64) -> VolumeLayout {
        VolumeLayout {
            rp,
            ttl,
            volume_size_limit,
            writable_volumes: Vec::new(),
            readonly_volumes: HashSet::new(),
            oversize_volumes: HashSet::new(),
            locations: HashMap::new(),
            locations_tx: HashMap::new(),
        }
    }

    // get match data_center, rack, node volume count
    #[deprecated]
    pub async fn active_volume_count(&self, option: &VolumeGrowOption) -> i64 {
        if option.data_center.is_empty() {
            return self.writable_volumes.len() as i64;
        }
        let mut count = 0;

        for vid in &self.writable_volumes {
            if let Some(nodes) = self.locations.get(vid) {
                for node in nodes {
                    let node = node.lock().await;
                    if node.id == option.data_node
                        && node.rack_id().await == option.rack
                        && node.data_center_id().await == option.data_center
                    {
                        count += 1;
                    }
                }
            }
        }

        count
    }

    pub async fn active_volume_count2(&self, option: &VolumeGrowOption) -> Result<i64> {
        if option.data_center.is_empty() {
            return Ok(self.writable_volumes.len() as i64);
        }
        let mut count = 0;

        for vid in &self.writable_volumes {
            if let Some(nodes) = self.locations_tx.get(vid) {
                for node in nodes {
                    if node.id().await? == option.data_node
                        && node.rack_id().await? == option.rack
                        && node.data_center_id().await? == option.data_center
                    {
                        count += 1;
                    }
                }
            }
        }

        Ok(count)
    }

    #[deprecated]
    pub async fn pick_for_write(
        &self,
        option: &VolumeGrowOption,
    ) -> Result<(VolumeId, Vec<Arc<Mutex<DataNode>>>)> {
        if self.writable_volumes.is_empty() {
            return Err(Error::NoWritableVolumes);
        }

        let mut counter = 0;
        let mut ret = (0, vec![]);

        for vid in &self.writable_volumes {
            if let Some(locations) = self.locations.get(vid) {
                for node in locations {
                    let dn = node.lock().await;
                    if !option.data_center.is_empty()
                        && option.data_center != dn.data_center_id().await
                        || !option.rack.is_empty() && option.rack != dn.rack_id().await
                        || !option.data_node.is_empty() && option.data_node != dn.id
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

        Err(Error::NoWritableVolumes)
    }

    pub async fn pick_for_write2(
        &self,
        option: &VolumeGrowOption,
    ) -> Result<(VolumeId, Vec<DataNodeEventTx>)> {
        if self.writable_volumes.is_empty() {
            return Err(Error::NoWritableVolumes);
        }

        let mut counter = 0;
        let mut ret = (0, vec![]);

        for vid in &self.writable_volumes {
            if let Some(locations) = self.locations_tx.get(vid) {
                for node in locations {
                    if !option.data_center.is_empty()
                        && option.data_center != node.data_center_id().await?
                        || !option.rack.is_empty() && option.rack != node.rack_id().await?
                        || !option.data_node.is_empty() && option.data_node != node.id().await?
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

        Err(Error::NoWritableVolumes)
    }

    #[deprecated]
    async fn set_node(locations: &mut Vec<Arc<Mutex<DataNode>>>, dn: Arc<Mutex<DataNode>>) {
        let mut same: Option<usize> = None;
        let mut i = 0;
        for location in locations.iter() {
            let location = location.lock().await;
            let dn = dn.lock().await;
            if location.ip != dn.ip || location.port != dn.port {
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
    }

    async fn set_node2(locations: &mut Vec<DataNodeEventTx>, dn: DataNodeEventTx) -> Result<()> {
        let mut same: Option<usize> = None;
        let mut i = 0;
        for location in locations.iter() {
            if location.ip().await? != dn.ip().await? || location.port().await? != dn.port().await?
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

    #[deprecated]
    pub async fn register_volume(&mut self, v: &VolumeInfo, dn: Arc<Mutex<DataNode>>) {
        {
            let list = self.locations.entry(v.id).or_default();
            VolumeLayout::set_node(list, dn.clone()).await;
        }

        let list = match self.locations.get(&v.id) {
            Some(locations) => locations.clone(),
            None => Vec::new(),
        };

        for node in list.iter() {
            match node.lock().await.volumes.get(&v.id) {
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

        if list.len() == self.rp.get_copy_count() && self.is_writable(v) {
            if self.oversize_volumes.get(&v.id).is_none() {
                self.add_to_writable(v.id);
            }
        } else {
            self.remove_from_writable(v.id);
            self.set_oversize_if_need(v);
        }
    }

    pub async fn register_volume2(&mut self, v: &VolumeInfo, dn: DataNodeEventTx) -> Result<()> {
        {
            let list = self.locations_tx.entry(v.id).or_default();
            VolumeLayout::set_node2(list, dn).await?;
        }

        let mut locations = vec![];
        if let Some(list) = self.locations_tx.get(&v.id) {
            locations.extend_from_slice(list);
        }

        for location in locations.iter() {
            match location.get_volume(v.id).await? {
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

        if locations.len() == self.rp.get_copy_count() && self.is_writable(v) {
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

    fn add_to_writable(&mut self, vid: VolumeId) {
        for id in self.writable_volumes.iter() {
            if *id == vid {
                return;
            }
        }
        self.writable_volumes.push(vid);
    }

    fn remove_from_writable(&mut self, vid: VolumeId) {
        let mut idx: Option<usize> = None;
        for (i, v) in self.writable_volumes.iter().enumerate() {
            if *v == vid {
                idx = Some(i);
                break;
            }
        }

        if let Some(idx) = idx {
            self.writable_volumes.remove(idx);
        }
    }

    pub fn unregister_volume(&mut self, v: &VolumeInfo) {
        self.remove_from_writable(v.id);
    }

    #[deprecated]
    pub fn lookup(&self, vid: VolumeId) -> Option<Vec<Arc<Mutex<DataNode>>>> {
        self.locations.get(&vid).cloned()
    }

    pub fn lookup2(&self, vid: VolumeId) -> Option<Vec<DataNodeEventTx>> {
        self.locations_tx.get(&vid).cloned()
    }
}
