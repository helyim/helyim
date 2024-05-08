use dashmap::DashMap;
use faststr::FastStr;
use helyim_proto::volume::AllocateVolumeRequest;
use rand::Rng;
use tracing::{debug, error};

use crate::{
    storage::{ReplicaPlacement, Ttl, VolumeError, VolumeId, VolumeInfo, CURRENT_VERSION},
    topology::{data_center::DataCenterRef, rack::RackRef, DataNodeRef, Topology},
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
        topology: &Topology,
    ) -> Result<Vec<DataNodeRef>, VolumeError> {
        let mut ret = vec![];

        let data_centers = &topology.data_centers;
        let (main_data_node, other_centers) = find_main_data_center(data_centers, option).await?;

        let racks = &main_data_node.racks;
        let (main_rack, other_racks) = find_main_rack(racks, option).await?;

        let data_nodes = &main_rack.data_nodes;
        let (main_dn, other_nodes) = find_main_node(data_nodes, option).await?;

        ret.push(main_dn);
        for nd in other_nodes {
            ret.push(nd.clone());
        }

        for rack in other_racks {
            let node = rack.reserve_one_volume().await?;
            ret.push(node);
        }

        for dc in other_centers {
            let node = dc.reserve_one_volume().await?;
            ret.push(node);
        }

        Ok(ret)
    }

    async fn find_and_grow(
        &self,
        option: &VolumeGrowOption,
        topology: &Topology,
    ) -> Result<usize, VolumeError> {
        let nodes = self.find_empty_slots(option, topology).await?;
        let len = nodes.len();
        let vid = topology.next_volume_id().await?;
        self.grow(vid, option, topology, nodes).await?;
        Ok(len)
    }

    async fn grow_by_count_and_type(
        &self,
        count: usize,
        option: &VolumeGrowOption,
        topology: &Topology,
    ) -> Result<usize, VolumeError> {
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

            dn.add_or_update_volume(&volume_info).await;
            topology.register_volume_layout(&volume_info, &dn).await;
        }
        Ok(())
    }
}

impl VolumeGrowth {
    pub async fn grow_by_type(
        &self,
        option: &VolumeGrowOption,
        topology: &Topology,
    ) -> Result<usize, VolumeError> {
        let count = self.find_volume_count(option.replica_placement.copy_count());
        self.grow_by_count_and_type(count, option, topology).await
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
) -> Result<(DataCenterRef, Vec<DataCenterRef>), VolumeError> {
    let mut candidates = vec![];
    let rp = option.replica_placement;

    for data_center in data_centers.iter() {
        if !option.data_center.is_empty() && data_center.id() != option.data_center {
            continue;
        }
        let racks_len = data_center.racks.len();
        if racks_len < rp.diff_rack_count as usize + 1 {
            continue;
        }
        if data_center.free_space() < (rp.diff_rack_count + rp.same_rack_count) as i64 + 1 {
            continue;
        }
        let mut possible_racks_count = 0;
        for rack in data_center.racks.iter() {
            let mut possible_nodes_count = 0;
            for dn in rack.data_nodes.iter() {
                if dn.free_space() >= 1 {
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
    candidates = vec![];

    for dc in data_centers.iter() {
        if dc.id == main_dc.id {
            continue;
        }
        if dc.free_space() <= 0 {
            continue;
        }
        candidates.push(dc.clone());
    }

    let mut rng = rand::thread_rng();
    let mut ret = rest_nodes.is_empty();

    for (i, data_center) in candidates.iter().enumerate() {
        if i < rest_nodes.len() {
            rest_nodes[i] = data_center.clone();
            if i == rest_nodes.len() - 1 {
                ret = true;
            }
        } else {
            let r = rng.gen_range(0..(i + 1));
            if r < rest_nodes.len() {
                rest_nodes[r] = data_center.clone();
            }
        }
    }

    if !ret {
        error!(
            "failed to pick {} data center from rest",
            rp.diff_data_center_count
        );
        return Err(VolumeError::NoFreeSpace(
            "not enough data center found".to_string(),
        ));
    }

    Ok((main_dc, rest_nodes))
}

async fn find_main_rack(
    racks: &DashMap<FastStr, RackRef>,
    option: &VolumeGrowOption,
) -> Result<(RackRef, Vec<RackRef>), VolumeError> {
    let mut candidates = vec![];
    let rp = option.replica_placement;

    for rack in racks.iter() {
        if !option.rack.is_empty() && option.rack != rack.id {
            continue;
        }
        if rack.free_space() < rp.same_rack_count as i64 + 1 {
            continue;
        }
        let data_nodes_len = rack.data_nodes.len();
        if data_nodes_len < rp.same_rack_count as usize + 1 {
            continue;
        }
        let mut possible_nodes = 0;
        for node in rack.data_nodes.iter() {
            if node.free_space() >= 1 {
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
    candidates = vec![];

    for rack in racks.iter() {
        if rack.id == main_rack.id {
            continue;
        }
        if rack.free_space() <= 0 {
            continue;
        }
        candidates.push(rack.clone());
    }

    let mut rng = rand::thread_rng();
    let mut ret = rest_nodes.is_empty();

    for (i, rack) in candidates.iter().enumerate() {
        if i < rest_nodes.len() {
            rest_nodes[i] = rack.clone();
            if i == rest_nodes.len() - 1 {
                ret = true;
            }
        } else {
            let r = rng.gen_range(0..(i + 1));
            if r < rest_nodes.len() {
                rest_nodes[r] = rack.clone();
            }
        }
    }

    if !ret {
        error!("failed to pick {} rack from rest", rp.diff_rack_count);
        return Err(VolumeError::NoFreeSpace(
            "not enough rack found".to_string(),
        ));
    }
    Ok((main_rack, rest_nodes))
}

async fn find_main_node(
    data_nodes: &DashMap<FastStr, DataNodeRef>,
    option: &VolumeGrowOption,
) -> Result<(DataNodeRef, Vec<DataNodeRef>), VolumeError> {
    let mut candidates = vec![];
    let rp = option.replica_placement;

    for node in data_nodes.iter() {
        let node_id = node.key();
        if !option.data_node.is_empty() && option.data_node != *node_id {
            continue;
        }
        if node.free_space() < 1 {
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
    candidates = vec![];

    for node in data_nodes.iter() {
        if node.id == main_dn.id {
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
            rest_nodes[i] = data_node.clone();
            if i == rest_nodes.len() - 1 {
                ret = true;
            }
        } else {
            let r = rng.gen_range(0..(i + 1));
            if r < rest_nodes.len() {
                rest_nodes[r] = data_node.clone();
            }
        }
    }

    if !ret {
        error!("failed to pick {} data node from rest", rp.same_rack_count);
        return Err(VolumeError::NoFreeSpace(
            "not enough data node found".to_string(),
        ));
    }

    Ok((main_dn, rest_nodes))
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use dashmap::DashMap;
    use faststr::FastStr;
    use serde::Deserialize;

    use crate::{
        directory::Sequencer,
        sequence::MemorySequencer,
        storage::{ReplicaPlacement, VolumeId, VolumeInfo, CURRENT_VERSION},
        topology::{
            data_center::DataCenter,
            data_node::DataNode,
            rack::Rack,
            volume_grow::{find_main_node, VolumeGrowOption, VolumeGrowth},
            DataNodeRef, Topology,
        },
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
        let data_nodes = DashMap::new();
        data_nodes.insert(
            FastStr::new("127.0.0.1:9333"),
            data_node(0, "127.0.0.1", 9333),
        );
        data_nodes.insert(
            FastStr::new("127.0.0.2:9333"),
            data_node(1, "127.0.0.2", 9333),
        );
        data_nodes.insert(
            FastStr::new("127.0.0.3:9333"),
            data_node(2, "127.0.0.3", 9333),
        );

        let option = VolumeGrowOption::default();

        let mut data_node1_cnt = 0;
        let mut data_node2_cnt = 0;
        let mut data_node3_cnt = 0;

        for _ in 0..1_000_000 {
            let (main_node, _rest_nodes) = find_main_node(&data_nodes, &option).await.unwrap();
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

    const TOPO_LAYOUT: &str = r#"
    {
      "dc1":{
        "rack1":{
          "server111":{
            "volumes":[
              {"id":1, "size":12312},
              {"id":2, "size":12312},
              {"id":3, "size":12312}
            ],
            "limit":3
          },
          "server112":{
            "volumes":[
              {"id":4, "size":12312},
              {"id":5, "size":12312},
              {"id":6, "size":12312}
            ],
            "limit":10
          }
        },
        "rack2":{
          "server121":{
            "volumes":[
              {"id":4, "size":12312},
              {"id":5, "size":12312},
              {"id":6, "size":12312}
            ],
            "limit":4
          },
          "server122":{
            "volumes":[],
            "limit":4
          },
          "server123":{
            "volumes":[
              {"id":2, "size":12312},
              {"id":3, "size":12312},
              {"id":4, "size":12312}
            ],
            "limit":5
          }
        }
      },
      "dc2":{
      },
      "dc3":{
        "rack2":{
          "server321":{
            "volumes":[
              {"id":1, "size":12312},
              {"id":3, "size":12312},
              {"id":5, "size":12312}
            ],
            "limit":4
          }
        }
      }
    }"#;

    #[derive(Deserialize, Debug)]
    struct ServerLayout {
        volumes: Vec<VolumeLayout>,
        limit: i64,
    }

    #[derive(Deserialize, Debug)]
    struct VolumeLayout {
        id: u32,
        size: u64,
    }

    async fn setup_topo() -> Topology {
        let data: HashMap<String, HashMap<String, HashMap<String, ServerLayout>>> =
            serde_json::from_str(TOPO_LAYOUT).unwrap();
        let topo = Topology::new(Sequencer::Memory(MemorySequencer::new()), 32 * 1024, 5);
        for (k, v) in data {
            let dc = Arc::new(DataCenter::new(FastStr::new(k)));
            topo.link_data_center(dc.clone());

            for (k, v) in v {
                let rack = Arc::new(Rack::new(FastStr::new(k)));
                dc.link_rack(rack.clone()).await;

                for (k, v) in v {
                    let data_node = Arc::new(DataNode::new(
                        FastStr::new(k),
                        FastStr::empty(),
                        0,
                        FastStr::empty(),
                        0,
                    ));
                    rack.link_data_node(data_node.clone()).await;

                    for vl in v.volumes {
                        let vi = VolumeInfo {
                            id: vl.id,
                            size: vl.size,
                            version: CURRENT_VERSION,
                            ..Default::default()
                        };
                        data_node.add_or_update_volume(&vi).await;
                    }
                    data_node.adjust_max_volume_count(v.limit).await;
                }
            }
        }
        topo
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

        for server in servers {
            println!("assigned node: {}", server.id);
        }
    }
}
