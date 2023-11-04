use std::{collections::HashMap, sync::Arc};

use faststr::FastStr;
use helyim_proto::AllocateVolumeRequest;
use rand::{prelude::SliceRandom, random};

use crate::{
    errors::{Error, Result},
    storage::{ReplicaPlacement, Ttl, VolumeId, VolumeInfo, CURRENT_VERSION},
    topology::{DataCenter, DataNode, Rack, TopologyEventTx},
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

    async fn find_empty_slots(
        &self,
        option: &VolumeGrowOption,
        topology: TopologyEventTx,
    ) -> Result<Vec<Arc<DataNode>>> {
        let rp = option.replica_placement;

        let mut ret = vec![];

        let data_centers = topology.data_centers().await?;
        let (main_data_node, other_centers) =
            find_main_data_center(&data_centers, option, &rp).await?;
        for dc in other_centers {
            let node = dc.reserve_one_volume().await?;
            ret.push(node);
        }

        let racks = main_data_node.racks().await?;
        let (main_rack, other_racks) = find_main_rack(&racks, option, &rp).await?;
        for rack in other_racks {
            let node = rack.reserve_one_volume().await?;
            ret.push(node);
        }

        let data_nodes = main_rack.data_nodes().await?;
        let (main_dn, other_nodes) = find_main_node(&data_nodes, option, &rp).await?;

        ret.push(main_dn);
        for nd in other_nodes {
            ret.push(nd.clone());
        }

        Ok(ret)
    }

    async fn find_and_grow(
        &self,
        option: &VolumeGrowOption,
        topology: TopologyEventTx,
    ) -> Result<usize> {
        let nodes = self.find_empty_slots(option, topology.clone()).await?;
        let len = nodes.len();
        let vid = topology.next_volume_id().await?;
        self.grow(vid, option, topology, nodes).await?;
        Ok(len)
    }

    async fn grow_by_count_and_type(
        &self,
        count: usize,
        option: &VolumeGrowOption,
        topology: TopologyEventTx,
    ) -> Result<usize> {
        let mut grow_count = 0;
        for _ in 0..count {
            grow_count += self.find_and_grow(option, topology.clone()).await?;
        }

        Ok(grow_count)
    }

    async fn grow(
        &self,
        vid: VolumeId,
        option: &VolumeGrowOption,
        topology: TopologyEventTx,
        nodes: Vec<Arc<DataNode>>,
    ) -> Result<()> {
        for dn in nodes {
            dn.allocate_volume(AllocateVolumeRequest {
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

            dn.add_or_update_volume(volume_info.clone()).await?;

            topology.register_volume_layout(volume_info, dn).await?;
        }
        Ok(())
    }
}

impl VolumeGrowth {
    pub async fn grow_by_type(
        &self,
        option: Arc<VolumeGrowOption>,
        topology: TopologyEventTx,
    ) -> Result<usize> {
        let count = self.find_volume_count(option.replica_placement.copy_count());
        self.grow_by_count_and_type(count, option.as_ref(), topology)
            .await
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

async fn find_main_data_center(
    data_centers: &HashMap<FastStr, Arc<DataCenter>>,
    option: &VolumeGrowOption,
    rp: &ReplicaPlacement,
) -> Result<(Arc<DataCenter>, Vec<Arc<DataCenter>>)> {
    let mut main_dc: Option<Arc<DataCenter>> = None;
    let mut other_centers: Vec<Arc<DataCenter>> = vec![];
    let mut valid_main_counts = 0;

    for (_, data_node) in data_centers.iter() {
        if !option.data_center.is_empty() && data_node.id != option.data_center {
            continue;
        }
        let racks = data_node.racks().await?;
        if racks.len() < rp.diff_rack_count as usize + 1 {
            continue;
        }
        if data_node.free_volumes().await?
            < rp.diff_rack_count as i64 + rp.same_rack_count as i64 + 1
        {
            continue;
        }
        let mut possible_racks_count = 0;
        for (_, rack) in racks.iter() {
            let mut possible_nodes_count = 0;
            for (_, dn) in rack.data_nodes().await?.iter() {
                if dn.free_volumes().await? >= 1 {
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
            main_dc = Some(data_node.clone());
        }
    }
    if main_dc.is_none() {
        return Err(Error::NoFreeSpace(
            "find main data center failed".to_string(),
        ));
    }
    let main_data_node = main_dc.unwrap();
    if rp.diff_data_center_count > 0 {
        for (dc_id, data_node) in data_centers.iter() {
            if *dc_id == main_data_node.id || data_node.free_volumes().await? < 1 {
                continue;
            }
            other_centers.push(data_node.clone());
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
    Ok((main_data_node, other_centers))
}

async fn find_main_rack(
    racks: &HashMap<FastStr, Arc<Rack>>,
    option: &VolumeGrowOption,
    rp: &ReplicaPlacement,
) -> Result<(Arc<Rack>, Vec<Arc<Rack>>)> {
    let mut main_rack: Option<Arc<Rack>> = None;
    let mut other_racks: Vec<Arc<Rack>> = vec![];
    let mut valid_rack_count = 0;

    for (_, rack) in racks.iter() {
        if !option.rack.is_empty() && option.rack != rack.id {
            continue;
        }
        if rack.free_volumes().await? < rp.same_rack_count as i64 + 1 {
            continue;
        }
        let data_nodes = rack.data_nodes().await?;
        if data_nodes.len() < rp.same_rack_count as usize + 1 {
            continue;
        }
        let mut possible_nodes = 0;
        for (_, node) in data_nodes.iter() {
            if node.free_volumes().await? < 1 {
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
        return Err(Error::NoFreeSpace("find main rack failed".to_string()));
    }
    let main_rack = main_rack.unwrap();
    if rp.diff_rack_count > 0 {
        for (rack_id, rack) in racks.iter() {
            if *rack_id == main_rack.id || rack.free_volumes().await? < 1 {
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
    Ok((main_rack, other_racks))
}

async fn find_main_node(
    data_nodes: &HashMap<FastStr, Arc<DataNode>>,
    option: &VolumeGrowOption,
    rp: &ReplicaPlacement,
) -> Result<(Arc<DataNode>, Vec<Arc<DataNode>>)> {
    let mut main_dn: Option<Arc<DataNode>> = None;
    let mut other_nodes: Vec<Arc<DataNode>> = vec![];
    let mut valid_node = 0;

    for (node_id, node) in data_nodes.iter() {
        if !option.data_node.is_empty() && option.data_node != *node_id {
            continue;
        }
        if node.free_volumes().await? < 1 {
            continue;
        }

        valid_node += 1;
        if random::<u32>() % valid_node == 0 {
            main_dn = Some(node.clone());
        }
    }
    if main_dn.is_none() {
        return Err(Error::NoFreeSpace("find main data node failed".to_string()));
    }
    let main_dn = main_dn.unwrap();
    if rp.same_rack_count > 0 {
        for (node_id, node) in data_nodes.iter() {
            if *node_id == main_dn.id || node.free_volumes().await? < 1 {
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

    Ok((main_dn, other_nodes))
}
