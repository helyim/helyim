use std::sync::Arc;

use dashmap::DashMap;
use faststr::FastStr;
use helyim_common::{
    ttl::Ttl,
    types::{ReplicaPlacement, VolumeId},
    version::CURRENT_VERSION,
};
use helyim_proto::volume::AllocateVolumeRequest;
use rand::Rng;
use tracing::{debug, error};

use crate::{
    DataNodeRef,
    node::{Node, downcast_node},
    topology::{Topology, TopologyError},
    volume::VolumeInfo,
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

    /// # 1. find the main data node
    ///
    /// - 1.1 collect all data nodes that have 1 slots
    /// - 2.2 collect all racks that have rp.SameRackCount+1
    /// - 2.3 collect all data centers that have DiffRackCount+rp.SameRackCount+1
    ///
    /// # 2. find rest data nodes
    async fn find_empty_slots(
        &self,
        option: &VolumeGrowOption,
        topology: &Topology,
    ) -> Result<Vec<DataNodeRef>, TopologyError> {
        let mut ret = vec![];
        let rp = option.replica_placement;

        let (main_data_center, other_centers) = randomly_pick_nodes(
            topology.children(),
            |node| {
                if !option.data_center.is_empty() && node.id() != option.data_center {
                    return false;
                }
                let racks_len = node.children().len();
                if racks_len < rp.diff_rack_count as usize + 1 {
                    return false;
                }
                if node.free_space() < (rp.diff_rack_count + rp.same_rack_count) as i64 + 1 {
                    return false;
                }
                let mut possible_racks_count = 0;
                for rack in node.children().iter() {
                    let mut possible_nodes_count = 0;
                    for dn in rack.children().iter() {
                        if dn.free_space() >= 1 {
                            possible_nodes_count += 1;
                        }
                    }
                    if possible_nodes_count > rp.same_rack_count {
                        possible_racks_count += 1;
                    }
                }
                if possible_racks_count < rp.diff_rack_count + 1 {
                    return false;
                }
                true
            },
            rp.diff_data_center_count as usize,
        )
        .await?;

        let (main_rack, other_racks) = randomly_pick_nodes(
            main_data_center.children(),
            |node| {
                if !option.rack.is_empty() && option.rack != node.id() {
                    return false;
                }
                if node.free_space() < rp.same_rack_count as i64 + 1 {
                    return false;
                }
                let data_nodes_len = node.children().len();
                if data_nodes_len < rp.same_rack_count as usize + 1 {
                    return false;
                }
                let mut possible_nodes = 0;
                for node in node.children().iter() {
                    if node.free_space() >= 1 {
                        possible_nodes += 1;
                    }
                }
                if possible_nodes < rp.same_rack_count as usize + 1 {
                    return false;
                }
                true
            },
            rp.diff_rack_count as usize,
        )
        .await?;

        let (main_dn, other_nodes) = randomly_pick_nodes(
            main_rack.children(),
            |node| {
                let node_id = node.id();
                if !option.data_node.is_empty() && option.data_node != *node_id {
                    return false;
                }
                if node.free_space() < 1 {
                    return false;
                }
                true
            },
            rp.same_rack_count as usize,
        )
        .await?;

        ret.push(downcast_node(main_dn)?);
        for node in other_nodes.into_iter().flatten() {
            ret.push(downcast_node(node)?);
        }

        for rack in other_racks.into_iter().flatten() {
            let random = rand::thread_rng().gen_range(0..rack.free_space());
            let node = rack.reserve_one_volume(random)?;
            ret.push(node);
        }

        for dc in other_centers.into_iter().flatten() {
            let random = rand::thread_rng().gen_range(0..dc.free_space());
            let node = dc.reserve_one_volume(random)?;
            ret.push(node);
        }

        Ok(ret)
    }

    async fn find_and_grow(
        &self,
        option: &VolumeGrowOption,
        topology: &Topology,
    ) -> Result<usize, TopologyError> {
        let nodes = self.find_empty_slots(option, topology).await?;
        let len = nodes.len();
        let vid = topology.next_volume_id().await;
        self.grow(vid, option, topology, nodes).await?;
        Ok(len)
    }

    pub async fn automatic_grow_by_type(
        &self,
        option: &VolumeGrowOption,
        topology: &Topology,
        mut target_count: usize,
    ) -> Result<usize, TopologyError> {
        debug!("automatic grow by type: target count: {target_count}");

        let copy_count = option.replica_placement.copy_count();
        if target_count == 0 {
            target_count = self.find_volume_count(copy_count);
        }
        self.grow_by_count_and_type(target_count, option, topology)
            .await
    }

    pub async fn grow_by_count_and_type(
        &self,
        count: usize,
        option: &VolumeGrowOption,
        topology: &Topology,
    ) -> Result<usize, TopologyError> {
        let mut grow_count = 0;
        for _ in 0..count {
            grow_count += self.find_and_grow(option, topology).await?;
        }

        Ok(grow_count)
    }

    async fn grow(
        &self,
        vid: VolumeId,
        option: &VolumeGrowOption,
        topology: &Topology,
        nodes: Vec<DataNodeRef>,
    ) -> Result<(), TopologyError> {
        for dn in nodes {
            dn.allocate_volume(AllocateVolumeRequest {
                volume_id: vid,
                collection: option.collection.to_string(),
                replication: option.replica_placement.to_string(),
                ttl: option.ttl.to_string(),
                preallocate: option.preallocate,
            })
            .await
            .map_err(|err| TopologyError::Box(Box::new(err)))?;

            let volume_info = VolumeInfo {
                id: vid,
                size: 0,
                collection: option.collection.clone(),
                replica_placement: option.replica_placement,
                ttl: option.ttl,
                version: CURRENT_VERSION,
                ..Default::default()
            };

            dn.add_or_update_volume(&volume_info).await;
            topology.register_volume_layout(&volume_info, &dn).await;
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct VolumeGrowOption {
    pub collection: FastStr,
    pub replica_placement: ReplicaPlacement,
    pub ttl: Ttl,
    pub preallocate: u64,
    pub data_center: FastStr,
    pub rack: FastStr,
    pub data_node: FastStr,
}

async fn randomly_pick_nodes<F>(
    children: &DashMap<FastStr, Arc<dyn Node>>,
    filter: F,
    nodes_num: usize,
) -> Result<(Arc<dyn Node>, Vec<Option<Arc<dyn Node>>>), TopologyError>
where
    F: Fn(&Arc<dyn Node>) -> bool,
{
    let mut candidates = vec![];

    for node in children.iter() {
        if filter(node.value()) {
            candidates.push(node.clone());
        } else {
            continue;
        }
    }
    if candidates.is_empty() {
        return Err(TopologyError::NoFreeSpace(
            "find main node failed".to_string(),
        ));
    }
    let first_idx = rand::thread_rng().gen_range(0..candidates.len());
    let main_dn = candidates[first_idx].clone();
    debug!("picked main node: {}", main_dn.id());

    let mut rest_nodes = vec![None; nodes_num];
    candidates = vec![];

    for node in children.iter() {
        if node.id() == main_dn.id() {
            continue;
        }
        if node.free_space() <= 0 {
            continue;
        }
        candidates.push(node.clone());
    }

    let mut rng = rand::thread_rng();

    let mut ret = rest_nodes.is_empty();
    for (i, data_node) in candidates.iter().enumerate() {
        if i < rest_nodes.len() {
            rest_nodes[i] = Some(data_node.clone());
            if i == rest_nodes.len() - 1 {
                ret = true;
            }
        } else {
            let r = rng.gen_range(0..(i + 1));
            if r < rest_nodes.len() {
                rest_nodes[r] = Some(data_node.clone());
            }
        }
    }

    if !ret {
        error!("failed to pick {nodes_num} node from rest");
        return Err(TopologyError::NoFreeSpace(
            "not enough node found".to_string(),
        ));
    }

    Ok((main_dn, rest_nodes))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use dashmap::DashMap;
    use faststr::FastStr;
    use helyim_common::types::{ReplicaPlacement, VolumeId};

    use crate::{
        DataNodeRef,
        data_node::DataNode,
        node::{Node, downcast_node},
        topology::tests::setup_topo,
        volume::VolumeInfo,
        volume_grow::{VolumeGrowOption, VolumeGrowth, randomly_pick_nodes},
    };

    fn data_node(volume_id: VolumeId, ip: &str, port: u16) -> DataNodeRef {
        let id = FastStr::new(format!("{ip}:{port}"));
        let ip = FastStr::new(ip);
        let public_url = id.clone();
        let data_node = DataNode::new(id, ip, port, public_url, 1);
        data_node.volumes.insert(volume_id, VolumeInfo::default());
        Arc::new(data_node)
    }

    #[test]
    pub fn test_find_volume_count() {
        let vg = VolumeGrowth {};
        assert_eq!(vg.find_volume_count(1), 7);
        assert_eq!(vg.find_volume_count(2), 6);
        assert_eq!(vg.find_volume_count(3), 3);
        assert_eq!(vg.find_volume_count(4), 1);
        assert_eq!(vg.find_volume_count(5), 1);
    }

    #[tokio::test]
    pub async fn test_find_main_node() {
        let data_nodes: DashMap<FastStr, Arc<dyn Node>> = DashMap::new();
        data_nodes.insert(
            FastStr::new("127.0.0.1:8080"),
            data_node(0, "127.0.0.1", 8080),
        );
        data_nodes.insert(
            FastStr::new("127.0.0.2:8080"),
            data_node(1, "127.0.0.2", 8080),
        );
        data_nodes.insert(
            FastStr::new("127.0.0.3:8080"),
            data_node(2, "127.0.0.3", 8080),
        );

        let option = VolumeGrowOption::default();

        let mut data_node1_cnt = 0;
        let mut data_node2_cnt = 0;
        let mut data_node3_cnt = 0;

        for _ in 0..1_000_000 {
            let (main_node, _rest_nodes) = randomly_pick_nodes(
                &data_nodes,
                |node| {
                    let node_id = node.id();
                    if !option.data_node.is_empty() && option.data_node != *node_id {
                        return false;
                    }
                    if node.free_space() < 1 {
                        return false;
                    }
                    true
                },
                option.replica_placement.same_rack_count as usize,
            )
            .await
            .unwrap();
            let main_node = downcast_node(main_node).unwrap();
            if main_node.ip == "127.0.0.1" {
                data_node1_cnt += 1;
            } else if main_node.ip == "127.0.0.2" {
                data_node2_cnt += 1;
            } else if main_node.ip == "127.0.0.3" {
                data_node3_cnt += 1;
            } else {
                panic!("should not be here.");
            }
        }

        println!("node1: {data_node1_cnt}, node2: {data_node2_cnt}, node3: {data_node3_cnt}");

        // assert_eq!(data_node1_cnt / 1000, 333);
        // assert_eq!(data_node2_cnt / 1000, 333);
        // assert_eq!(data_node3_cnt / 1000, 333);
    }

    #[tokio::test]
    pub async fn test_find_empty_slots() {
        let topo = setup_topo().await;
        let vg = VolumeGrowth {};
        let rp = ReplicaPlacement::new("002").unwrap();

        let vgo = VolumeGrowOption {
            replica_placement: rp,
            data_center: FastStr::new("dc1"),
            ..Default::default()
        };

        let servers = vg.find_empty_slots(&vgo, &topo).await.unwrap();

        assert_eq!(servers.len(), 3);
        for server in servers {
            println!("assigned node: {}", server.id());
        }
    }
}
