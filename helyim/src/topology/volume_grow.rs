use std::sync::Arc;

use async_lock::RwLock;
use dashmap::DashMap;
use faststr::FastStr;
use helyim_proto::AllocateVolumeRequest;
use rand::{prelude::SliceRandom, random};

use crate::{
    errors::{Error, Result},
    storage::{ReplicaPlacement, Ttl, VolumeId, VolumeInfo, CURRENT_VERSION},
    topology::{DataCenter, DataNode, Rack, Topology},
};

#[derive(Debug, Clone)]
pub struct VolumeGrowth;

impl VolumeGrowth {
    /// one replication type may need rp.get_copy_count() actual volumes
    /// given copy_count, how many logical volumes to create
    fn find_volume_count(&self, count: usize) -> usize {
        match count {
            1 => 7,
            2 => 6,
            3 => 3,
            _ => 1,
        }
    }

    // TODO: too long func...
    // will specify data_node but no data center find a wrong data center to be the
    // main data center first, then no valid data_node ???
    async fn find_empty_slots(
        &self,
        option: &VolumeGrowOption,
        data_centers: Arc<DashMap<FastStr, Arc<DataCenter>>>,
    ) -> Result<Vec<Arc<RwLock<DataNode>>>> {
        let mut main_dc: Option<Arc<DataCenter>> = None;
        let mut main_rack: Option<Arc<RwLock<Rack>>> = None;
        let mut main_dn: Option<Arc<RwLock<DataNode>>> = None;
        let mut other_centers: Vec<Arc<DataCenter>> = vec![];
        let mut other_racks: Vec<Arc<RwLock<Rack>>> = vec![];
        let mut other_nodes: Vec<Arc<RwLock<DataNode>>> = vec![];

        let rp = option.replica_placement;
        let mut valid_main_counts = 0;
        // find main data center
        for data_center in data_centers.iter() {
            if !option.data_center.is_empty() && data_center.id != option.data_center {
                continue;
            }

            let racks = &data_center.racks;

            if racks.len() < rp.diff_rack_count as usize + 1 {
                continue;
            }

            if data_center.free_volumes().await?
                < rp.diff_rack_count as i64 + rp.same_rack_count as i64 + 1
            {
                continue;
            }

            let mut possible_racks_count = 0;
            for rack in racks.iter() {
                let mut possible_nodes_count = 0;
                for dn in rack.read().await.nodes.iter() {
                    if dn.read().await.free_volumes() >= 1 {
                        possible_nodes_count += 1;
                    }
                }

                if possible_nodes_count > rp.same_rack_count {
                    possible_racks_count += 1;
                }
            }

            if possible_racks_count < rp.diff_rack_count + 1 {
                continue;
            }

            valid_main_counts += 1;
            if random::<u32>() % valid_main_counts == 0 {
                main_dc = Some(data_center.clone());
            }
        }

        if main_dc.is_none() {
            return Err(Error::NoFreeSpace("find main dc fail".to_string()));
        }
        let main_data_center = main_dc.unwrap();

        if rp.diff_data_center_count > 0 {
            for entry in data_centers.iter() {
                let dc_id = entry.key();
                let data_center = entry.value();
                if *dc_id == main_data_center.id || data_center.free_volumes().await? < 1 {
                    continue;
                }
                other_centers.push(data_center.clone());
            }
        }
        if other_centers.len() < rp.diff_data_center_count as usize {
            return Err(Error::NoFreeSpace("no enough data centers".to_string()));
        }

        other_centers
            .as_mut_slice()
            .shuffle(&mut rand::thread_rng());
        let tmp_centers = other_centers
            .drain(0..rp.diff_data_center_count as usize)
            .collect();
        other_centers = tmp_centers;

        // find main rack
        let mut valid_rack_count = 0;
        for rack in main_data_center.racks.iter() {
            if !option.rack.is_empty() && option.rack != rack.read().await.id {
                continue;
            }

            if rack.read().await.free_volumes().await? < rp.same_rack_count as i64 + 1 {
                continue;
            }

            let data_nodes = &rack.read().await.nodes;

            if data_nodes.len() < rp.same_rack_count as usize + 1 {
                continue;
            }

            let mut possible_nodes = 0;
            for node in data_nodes.iter() {
                if node.read().await.free_volumes() < 1 {
                    continue;
                }

                possible_nodes += 1;
            }

            if possible_nodes < rp.same_rack_count as usize + 1 {
                continue;
            }
            valid_rack_count += 1;

            if random::<u32>() % valid_rack_count == 0 {
                main_rack = Some(rack.clone());
            }
        }

        if main_rack.is_none() {
            return Err(Error::NoFreeSpace("find main rack fail".to_string()));
        }

        let main_rack = main_rack.unwrap();

        if rp.diff_rack_count > 0 {
            for rack in main_data_center.racks.iter() {
                let rack_id = rack.key();
                let rack = rack.value();
                if *rack_id == main_rack.read().await.id
                    || rack.read().await.free_volumes().await? < 1
                {
                    continue;
                }
                other_racks.push(rack.clone());
            }
        }

        if other_racks.len() < rp.diff_rack_count as usize {
            return Err(Error::NoFreeSpace("no enough racks".to_string()));
        }

        other_racks.as_mut_slice().shuffle(&mut rand::thread_rng());

        let tmp_racks = other_racks.drain(0..rp.diff_rack_count as usize).collect();
        other_racks = tmp_racks;

        // find main node
        let mut valid_node = 0;
        for entry in main_rack.read().await.nodes.iter() {
            let node_id = entry.key();
            let node = entry.value();

            if !option.data_node.is_empty() && option.data_node != *node_id {
                continue;
            }
            if node.read().await.free_volumes() < 1 {
                continue;
            }

            valid_node += 1;
            if random::<u32>() % valid_node == 0 {
                main_dn = Some(node.clone());
            }
        }

        if main_dn.is_none() {
            return Err(Error::NoFreeSpace("find main node fail".to_string()));
        }
        let main_data_node = main_dn.unwrap().clone();

        if rp.same_rack_count > 0 {
            for entry in main_rack.read().await.nodes.iter() {
                let node_id = entry.key();
                let node = entry.value();

                if *node_id == main_data_node.read().await.id
                    || node.read().await.free_volumes() < 1
                {
                    continue;
                }
                other_nodes.push(node.clone());
            }
        }

        if other_nodes.len() < rp.same_rack_count as usize {
            return Err(Error::NoFreeSpace("no enough nodes".to_string()));
        }
        other_nodes.as_mut_slice().shuffle(&mut rand::thread_rng());
        let tmp_nodes = other_nodes.drain(0..rp.same_rack_count as usize).collect();
        other_nodes = tmp_nodes;

        let mut ret = vec![];
        ret.push(main_data_node.clone());

        for nd in other_nodes {
            ret.push(nd.clone());
        }

        for rack in other_racks {
            let node = rack.read().await.reserve_one_volume().await?;
            ret.push(node);
        }

        for dc in other_centers {
            let node = dc.reserve_one_volume().await?;
            ret.push(node);
        }

        Ok(ret)
    }

    async fn find_and_grow(
        &mut self,
        option: &VolumeGrowOption,
        topology: Arc<Topology>,
    ) -> Result<usize> {
        let nodes = self
            .find_empty_slots(option, topology.data_centers.clone())
            .await?;
        let len = nodes.len();
        let vid = topology.next_volume_id().await?;
        self.grow(vid, option, topology, nodes).await?;
        Ok(len)
    }

    async fn grow_by_count_and_type(
        &mut self,
        count: usize,
        option: &VolumeGrowOption,
        topology: Arc<Topology>,
    ) -> Result<usize> {
        let mut grow_count = 0;
        for _ in 0..count {
            grow_count += self.find_and_grow(option, topology.clone()).await?;
        }

        Ok(grow_count)
    }

    async fn grow(
        &mut self,
        vid: VolumeId,
        option: &VolumeGrowOption,
        topology: Arc<Topology>,
        nodes: Vec<Arc<RwLock<DataNode>>>,
    ) -> Result<()> {
        for dn in nodes {
            dn.write()
                .await
                .allocate_volume(AllocateVolumeRequest {
                    volumes: vec![vid],
                    collection: option.collection.to_string(),
                    replication: option.replica_placement.to_string(),
                    ttl: option.ttl.to_string(),
                    preallocate: option.preallocate,
                })
                .await?;

            let volume_info = VolumeInfo {
                id: vid,
                size: 0,
                collection: option.collection.clone(),
                replica_placement: option.replica_placement,
                ttl: option.ttl,
                version: CURRENT_VERSION,
                ..Default::default()
            };

            dn.write()
                .await
                .add_or_update_volume(volume_info.clone())
                .await?;

            topology.register_volume_layout(volume_info, dn).await?;
        }
        Ok(())
    }
}

impl VolumeGrowth {
    pub async fn grow_by_type(
        &mut self,
        option: VolumeGrowOption,
        topology: Arc<Topology>,
    ) -> Result<usize> {
        let count = self.find_volume_count(option.replica_placement.copy_count());
        self.grow_by_count_and_type(count, &option, topology).await
    }
}

#[derive(Debug, Default, Clone)]
pub struct VolumeGrowOption {
    pub collection: FastStr,
    pub replica_placement: ReplicaPlacement,
    pub ttl: Ttl,
    pub preallocate: i64,
    pub data_center: FastStr,
    pub rack: FastStr,
    pub data_node: FastStr,
}
