use std::sync::Arc;

use dashmap::DashMap;
use faststr::FastStr;
use helyim_proto::volume::AllocateVolumeRequest;
use rand::Rng;
use tracing::debug;

use crate::{
    storage::{ReplicaPlacement, Ttl, VolumeError, VolumeId, VolumeInfo, CURRENT_VERSION},
    topology::{data_center::DataCenterRef, rack::RackRef, DataNodeRef, TopologyRef},
};

#[derive(Debug, Copy, Clone)]
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

    /// 1. find the main data node
    /// 1.1 collect all data nodes that have 1 slots
    /// 2.2 collect all racks that have rp.SameRackCount+1
    /// 2.2 collect all data centers that have DiffRackCount+rp.SameRackCount+1
    /// 2. find rest data nodes
    async fn find_empty_slots(
        &self,
        option: &VolumeGrowOption,
        topology: TopologyRef,
    ) -> Result<Vec<DataNodeRef>, VolumeError> {
        let rp = option.replica_placement;

        let mut ret = vec![];

        let data_centers = &topology.data_centers;
        let (main_data_node, other_centers) =
            find_main_data_center(data_centers, option, &rp).await?;
        for dc in other_centers {
            let node = dc.reserve_one_volume().await?;
            ret.push(node);
        }

        let racks = &main_data_node.racks;
        let (main_rack, other_racks) = find_main_rack(racks, option, &rp).await?;
        for rack in other_racks {
            let node = rack.reserve_one_volume().await?;
            ret.push(node);
        }

        let data_nodes = &main_rack.data_nodes;
        let (main_dn, other_nodes) = find_main_node(data_nodes, option, &rp).await?;

        ret.push(main_dn);
        for nd in other_nodes {
            ret.push(nd.clone());
        }

        Ok(ret)
    }

    async fn find_and_grow(
        &self,
        option: &VolumeGrowOption,
        topology: &TopologyRef,
    ) -> Result<usize, VolumeError> {
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
        topology: TopologyRef,
    ) -> Result<usize, VolumeError> {
        let mut grow_count = 0;
        for _ in 0..count {
            grow_count += self.find_and_grow(option, &topology).await?;
        }

        Ok(grow_count)
    }

    async fn grow(
        &self,
        vid: VolumeId,
        option: &VolumeGrowOption,
        topology: &TopologyRef,
        nodes: Vec<DataNodeRef>,
    ) -> Result<(), VolumeError> {
        for dn in nodes {
            dn.allocate_volume(AllocateVolumeRequest {
                volume_id: vid,
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

            dn.add_or_update_volume(volume_info.clone()).await;
            topology.register_volume_layout(volume_info, dn).await;
        }
        Ok(())
    }
}

impl VolumeGrowth {
    pub async fn grow_by_type(
        &self,
        option: Arc<VolumeGrowOption>,
        topology: TopologyRef,
    ) -> Result<usize, VolumeError> {
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
    data_centers: &DashMap<FastStr, DataCenterRef>,
    option: &VolumeGrowOption,
    rp: &ReplicaPlacement,
) -> Result<(DataCenterRef, Vec<DataCenterRef>), VolumeError> {
    let mut candidates = vec![];

    for data_center in data_centers.iter() {
        if !option.data_center.is_empty() && data_center.id() != option.data_center {
            continue;
        }
        let racks_len = data_center.racks.len();
        if racks_len < rp.diff_rack_count as usize + 1 {
            continue;
        }
        if data_center.free_volumes() < (rp.diff_rack_count + rp.same_rack_count) as i64 + 1 {
            continue;
        }
        let mut possible_racks_count = 0;
        for rack in data_center.racks.iter() {
            let mut possible_nodes_count = 0;
            for dn in rack.data_nodes.iter() {
                if dn.free_volumes() >= 1 {
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
        candidates.push(data_center.clone());
    }

    if candidates.is_empty() {
        return Err(VolumeError::NoFreeSpace(
            "find main data center failed".to_string(),
        ));
    }

    let first_idx = rand::thread_rng().gen_range(0..candidates.len());
    let main_dc = candidates[first_idx].clone();
    debug!("picked main data center: {}", main_dc.id());

    let mut rest_nodes = Vec::with_capacity(rp.diff_data_center_count as usize);
    candidates.remove(first_idx);

    let mut rng = rand::thread_rng();
    for (i, data_center) in candidates.iter().enumerate() {
        if i < rest_nodes.len() {
            rest_nodes.insert(i, data_center.clone());
        } else {
            let r = rng.gen_range(0..(i + 1));
            if r < rest_nodes.len() {
                rest_nodes.insert(r, data_center.clone());
            }
        }
    }

    Ok((main_dc, rest_nodes))
}

async fn find_main_rack(
    racks: &DashMap<FastStr, RackRef>,
    option: &VolumeGrowOption,
    rp: &ReplicaPlacement,
) -> Result<(RackRef, Vec<RackRef>), VolumeError> {
    let mut candidates = vec![];

    for rack in racks.iter() {
        if !option.rack.is_empty() && option.rack != rack.id {
            continue;
        }
        if rack.free_volumes() < rp.same_rack_count as i64 + 1 {
            continue;
        }
        let data_nodes_len = rack.data_nodes.len();
        if data_nodes_len < rp.same_rack_count as usize + 1 {
            continue;
        }
        let mut possible_nodes = 0;
        for node in rack.data_nodes.iter() {
            if node.free_volumes() >= 1 {
                possible_nodes += 1;
            }
        }
        if possible_nodes < rp.same_rack_count as usize + 1 {
            continue;
        }
        candidates.push(rack.clone());
    }

    if candidates.is_empty() {
        return Err(VolumeError::NoFreeSpace(
            "find main rack failed".to_string(),
        ));
    }

    let first_idx = rand::thread_rng().gen_range(0..candidates.len());
    let main_rack = candidates[first_idx].clone();
    debug!("picked main rack: {}", main_rack.id());

    let mut rest_nodes = Vec::with_capacity(rp.diff_rack_count as usize);
    candidates.remove(first_idx);

    let mut rng = rand::thread_rng();
    for (i, rack) in candidates.iter().enumerate() {
        if i < rest_nodes.len() {
            rest_nodes.insert(i, rack.clone());
        } else {
            let r = rng.gen_range(0..(i + 1));
            if r < rest_nodes.len() {
                rest_nodes.insert(r, rack.clone());
            }
        }
    }
    Ok((main_rack, rest_nodes))
}

async fn find_main_node(
    data_nodes: &DashMap<FastStr, DataNodeRef>,
    option: &VolumeGrowOption,
    rp: &ReplicaPlacement,
) -> Result<(DataNodeRef, Vec<DataNodeRef>), VolumeError> {
    let mut candidates = vec![];

    for node in data_nodes.iter() {
        let node_id = node.key();
        if !option.data_node.is_empty() && option.data_node != *node_id {
            continue;
        }
        if node.free_volumes() < 1 {
            continue;
        }
        candidates.push(node.clone());
    }
    if candidates.is_empty() {
        return Err(VolumeError::NoFreeSpace(
            "find main data node failed".to_string(),
        ));
    }
    let first_idx = rand::thread_rng().gen_range(0..candidates.len());
    let main_dn = candidates[first_idx].clone();
    debug!("picked main data node: {}", main_dn.id());

    let mut rest_nodes = Vec::with_capacity(rp.same_rack_count as usize);
    candidates.remove(first_idx);

    let mut rng = rand::thread_rng();
    for (i, data_node) in candidates.iter().enumerate() {
        if i < rest_nodes.len() {
            rest_nodes.insert(i, data_node.clone());
        } else {
            let r = rng.gen_range(0..(i + 1));
            if r < rest_nodes.len() {
                rest_nodes.insert(r, data_node.clone());
            }
        }
    }

    Ok((main_dn, rest_nodes))
}
