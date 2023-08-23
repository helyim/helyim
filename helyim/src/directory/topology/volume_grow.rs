use std::{ops::Deref, sync::Arc};

use futures::lock::Mutex;
use rand::{prelude::SliceRandom, random};
use serde_json::Value;

use crate::{
    directory::topology::{
        data_center::DataCenterEventTx, topology::Topology, DataCenter, DataNode, DataNodeEventTx,
        Rack, RackEventTx,
    },
    errors::{Error, Result},
    storage::{ReplicaPlacement, Ttl, VolumeId, VolumeInfo, CURRENT_VERSION},
    util,
};

#[derive(Debug, Copy, Clone, Default)]
pub struct VolumeGrowth;

impl VolumeGrowth {
    pub fn new() -> Self {
        Self
    }

    #[deprecated]
    pub async fn grow_by_type(
        &self,
        option: &VolumeGrowOption,
        topology: &mut Topology,
    ) -> Result<usize> {
        let count = self.find_volume_count(option.replica_placement.get_copy_count());
        self.grow_by_count_and_type(count, option, topology).await
    }

    pub async fn grow_by_type2(
        &self,
        option: &VolumeGrowOption,
        topology: &mut Topology,
    ) -> Result<usize> {
        let count = self.find_volume_count(option.replica_placement.get_copy_count());
        self.grow_by_count_and_type2(count, option, topology).await
    }

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
    #[deprecated]
    async fn find_empty_slots(
        &self,
        option: &VolumeGrowOption,
        topology: &Topology,
    ) -> Result<Vec<Arc<Mutex<DataNode>>>> {
        let mut main_dc: Option<Arc<Mutex<DataCenter>>> = None;
        let mut main_rack: Option<Arc<Mutex<Rack>>> = None;
        let mut main_nd: Option<Arc<Mutex<DataNode>>> = None;
        let mut other_centers: Vec<Arc<Mutex<DataCenter>>> = vec![];
        let mut other_racks: Vec<Arc<Mutex<Rack>>> = vec![];
        let mut other_nodes: Vec<Arc<Mutex<DataNode>>> = vec![];

        let rp = option.replica_placement;
        let mut valid_main_counts = 0;
        // find main data center
        for (_dc_id, dc_arc) in topology.data_centers.iter() {
            let dc = dc_arc.lock().await;
            if !option.data_center.is_empty() && dc.id != option.data_center {
                continue;
            }

            if dc.racks.len() < rp.diff_rack_count as usize + 1 {
                continue;
            }

            if dc.free_volumes().await < rp.diff_rack_count as i64 + rp.same_rack_count as i64 + 1 {
                continue;
            }

            let mut possible_racks_count = 0;
            for (_rack_id, rack_arc) in dc.racks.iter() {
                let rack = rack_arc.lock().await;
                let mut possible_nodes_count = 0;
                for (_, nd) in rack.nodes.iter() {
                    if nd.lock().await.free_volumes() >= 1 {
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
                main_dc = Some(dc_arc.clone());
            }
        }

        if main_dc.is_none() {
            return Err(Error::NoFreeSpace("find main dc fail".to_string()));
        }
        let main_dc_arc = main_dc.unwrap();

        if rp.diff_data_center_count > 0 {
            for (dc_id, dc_arc) in topology.data_centers.iter() {
                let dc = dc_arc.lock().await;
                if *dc_id == main_dc_arc.lock().await.id || dc.free_volumes().await < 1 {
                    continue;
                }
                other_centers.push(dc_arc.clone());
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
        for (_rack_id, rack_arc) in main_dc_arc.lock().await.racks.iter() {
            let rack = rack_arc.lock().await;
            if !option.rack.is_empty() && option.rack != rack.id {
                continue;
            }

            if rack.free_volumes().await < rp.same_rack_count as i64 + 1 {
                continue;
            }

            if rack.nodes.len() < rp.same_rack_count as usize + 1 {
                continue;
            }

            let mut possible_nodes = 0;
            for (_node_id, node) in rack.nodes.iter() {
                if node.lock().await.free_volumes() < 1 {
                    continue;
                }

                possible_nodes += 1;
            }

            if possible_nodes < rp.same_rack_count as usize + 1 {
                continue;
            }
            valid_rack_count += 1;

            if random::<u32>() % valid_rack_count == 0 {
                main_rack = Some(rack_arc.clone());
            }
        }

        if main_rack.is_none() {
            return Err(Error::NoFreeSpace("find main rack fail".to_string()));
        }

        let main_rack_arc = main_rack.unwrap();

        if rp.diff_rack_count > 0 {
            for (rack_id, rack_arc) in main_dc_arc.lock().await.racks.iter() {
                let rack = rack_arc.lock().await;
                if *rack_id == main_rack_arc.lock().await.id || rack.free_volumes().await < 1 {
                    continue;
                }
                other_racks.push(rack_arc.clone());
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
        for (node_id, node) in main_rack_arc.lock().await.nodes.iter() {
            if !option.data_node.is_empty() && option.data_node != *node_id {
                continue;
            }
            if node.lock().await.free_volumes() < 1 {
                continue;
            }

            valid_node += 1;
            if random::<u32>() % valid_node == 0 {
                main_nd = Some(node.clone());
            }
        }

        if main_nd.is_none() {
            return Err(Error::NoFreeSpace("find main node fail".to_string()));
        }
        let main_nd_arc = main_nd.unwrap().clone();

        if rp.same_rack_count > 0 {
            for (node_id, node) in main_rack_arc.lock().await.nodes.iter() {
                let node_ref = node.lock().await;

                if *node_id == main_nd_arc.lock().await.id || node_ref.free_volumes() < 1 {
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
        ret.push(main_nd_arc.clone());

        for nd in other_nodes {
            ret.push(nd.clone());
        }

        for rack in other_racks {
            let node = rack.lock().await.reserve_one_volume().await?;
            ret.push(node);
        }

        for dc in other_centers {
            let node = dc.lock().await.reserve_one_volume().await?;
            ret.push(node);
        }

        Ok(ret)
    }

    // TODO: too long func...
    // will specify data_node but no data center find a wrong data center to be the
    // main data center first, then no valid data_node ???
    async fn find_empty_slots2(
        &self,
        option: &VolumeGrowOption,
        topology: &Topology,
    ) -> Result<Vec<DataNodeEventTx>> {
        let mut main_dc: Option<DataCenterEventTx> = None;
        let mut main_rack: Option<RackEventTx> = None;
        let mut main_dn: Option<DataNodeEventTx> = None;
        let mut other_centers: Vec<DataCenterEventTx> = vec![];
        let mut other_racks: Vec<RackEventTx> = vec![];
        let mut other_nodes: Vec<DataNodeEventTx> = vec![];

        let rp = option.replica_placement;
        let mut valid_main_counts = 0;
        // find main data center
        for (_, dc_tx) in topology.data_centers_tx.iter() {
            if !option.data_center.is_empty() && dc_tx.id().await? != option.data_center {
                continue;
            }

            let racks = dc_tx.racks().await?;

            if racks.len() < rp.diff_rack_count as usize + 1 {
                continue;
            }

            if dc_tx.free_volumes().await?
                < rp.diff_rack_count as i64 + rp.same_rack_count as i64 + 1
            {
                continue;
            }

            let mut possible_racks_count = 0;
            for (_, rack_tx) in racks.iter() {
                let mut possible_nodes_count = 0;
                for (_, dn) in rack_tx.data_nodes().await?.iter() {
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
                main_dc = Some(dc_tx.clone());
            }
        }

        if main_dc.is_none() {
            return Err(Error::NoFreeSpace("find main dc fail".to_string()));
        }
        let main_dc_tx = main_dc.unwrap();

        if rp.diff_data_center_count > 0 {
            for (dc_id, dc_tx) in topology.data_centers_tx.iter() {
                if *dc_id == main_dc_tx.id().await? || dc_tx.free_volumes().await? < 1 {
                    continue;
                }
                other_centers.push(dc_tx.clone());
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
        for (_, rack_tx) in main_dc_tx.racks().await?.iter() {
            if !option.rack.is_empty() && option.rack != rack_tx.id().await? {
                continue;
            }

            if rack_tx.free_volumes().await? < rp.same_rack_count as i64 + 1 {
                continue;
            }

            let data_nodes = rack_tx.data_nodes().await?;

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
                main_rack = Some(rack_tx.clone());
            }
        }

        if main_rack.is_none() {
            return Err(Error::NoFreeSpace("find main rack fail".to_string()));
        }

        let main_rack_tx = main_rack.unwrap();

        if rp.diff_rack_count > 0 {
            for (rack_id, rack_tx) in main_dc_tx.racks().await?.iter() {
                if *rack_id == main_rack_tx.id().await? || rack_tx.free_volumes().await? < 1 {
                    continue;
                }
                other_racks.push(rack_tx.clone());
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
        for (node_id, node) in main_rack_tx.data_nodes().await?.iter() {
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
            return Err(Error::NoFreeSpace("find main node fail".to_string()));
        }
        let main_dn_tx = main_dn.unwrap().clone();

        if rp.same_rack_count > 0 {
            for (node_id, node) in main_rack_tx.data_nodes().await?.iter() {
                if *node_id == main_dn_tx.id().await? || node.free_volumes().await? < 1 {
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
        ret.push(main_dn_tx.clone());

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

    #[deprecated]
    async fn find_and_grow(
        &self,
        option: &VolumeGrowOption,
        topology: &mut Topology,
    ) -> Result<usize> {
        let nodes = self.find_empty_slots(option, topology).await?;
        let len = nodes.len();
        let vid = topology.next_volume_id().await?;
        self.grow(vid, option, topology, nodes).await?;
        Ok(len)
    }

    async fn find_and_grow2(
        &self,
        option: &VolumeGrowOption,
        topology: &mut Topology,
    ) -> Result<usize> {
        let nodes = self.find_empty_slots2(option, topology).await?;
        let len = nodes.len();
        let vid = topology.next_volume_id().await?;
        self.grow2(vid, option, topology, nodes).await?;
        Ok(len)
    }

    #[deprecated]
    async fn grow_by_count_and_type(
        &self,
        count: usize,
        option: &VolumeGrowOption,
        topology: &mut Topology,
    ) -> Result<usize> {
        let mut grow_count = 0;
        for _ in 0..count {
            grow_count += self.find_and_grow(option, topology).await?;
        }

        Ok(grow_count)
    }

    async fn grow_by_count_and_type2(
        &self,
        count: usize,
        option: &VolumeGrowOption,
        topology: &mut Topology,
    ) -> Result<usize> {
        let mut grow_count = 0;
        for _ in 0..count {
            grow_count += self.find_and_grow2(option, topology).await?;
        }

        Ok(grow_count)
    }

    #[deprecated]
    async fn grow(
        &self,
        vid: VolumeId,
        option: &VolumeGrowOption,
        topology: &mut Topology,
        nodes: Vec<Arc<Mutex<DataNode>>>,
    ) -> Result<()> {
        for nd in nodes {
            allocate_volume(nd.lock().await.deref(), vid, option).await?;
            let volume_info = VolumeInfo {
                id: vid,
                size: 0,
                collection: option.collection.clone(),
                replica_placement: option.replica_placement,
                ttl: option.ttl,
                version: CURRENT_VERSION,
                ..Default::default()
            };

            {
                let mut node = nd.lock().await;
                node.add_or_update_volume(volume_info.clone()).await;
            }

            topology
                .register_volume_layout(volume_info, nd.clone())
                .await;
        }
        Ok(())
    }

    async fn grow2(
        &self,
        vid: VolumeId,
        option: &VolumeGrowOption,
        topology: &mut Topology,
        nodes: Vec<DataNodeEventTx>,
    ) -> Result<()> {
        for dn in nodes {
            allocate_volume2(&dn, vid, option).await?;
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

            topology.register_volume_layout2(volume_info, dn).await?;
        }
        Ok(())
    }
}

pub enum VolumeGrowthEvent {
    GrowByType
}

#[derive(Debug, Default)]
pub struct VolumeGrowOption {
    pub collection: String,
    pub replica_placement: ReplicaPlacement,
    pub ttl: Ttl,
    pub preallocate: i64,
    pub data_center: String,
    pub rack: String,
    pub data_node: String,
}

#[deprecated]
async fn allocate_volume(dn: &DataNode, vid: VolumeId, option: &VolumeGrowOption) -> Result<()> {
    let vid = &vid.to_string();
    let collection = &option.collection;
    let rp = &option.replica_placement.to_string();
    let ttl = &option.ttl.to_string();
    let preallocate = &format!("{}", option.preallocate);

    let params: Vec<(&str, &str)> = vec![
        ("volume", vid),
        ("collection", collection),
        ("replication", rp),
        ("ttl", ttl),
        ("preallocate", preallocate),
    ];

    let body = util::get(&format!("http://{}/admin/assign_volume", dn.url()), &params).await?;

    let v: Value = serde_json::from_slice(&body)?;

    if let Value::String(ref s) = v["Error"] {
        return Err(Error::String(s.clone()));
    }

    Ok(())
}

async fn allocate_volume2(
    dn: &DataNodeEventTx,
    vid: VolumeId,
    option: &VolumeGrowOption,
) -> Result<()> {
    let vid = &vid.to_string();
    let collection = &option.collection;
    let rp = &option.replica_placement.to_string();
    let ttl = &option.ttl.to_string();
    let preallocate = &format!("{}", option.preallocate);

    let params: Vec<(&str, &str)> = vec![
        ("volume", vid),
        ("collection", collection),
        ("replication", rp),
        ("ttl", ttl),
        ("preallocate", preallocate),
    ];

    let body = util::get(
        &format!("http://{}/admin/assign_volume", dn.url().await?),
        &params,
    )
    .await?;

    let v: Value = serde_json::from_slice(&body)?;

    if let Value::String(ref s) = v["Error"] {
        return Err(Error::String(s.clone()));
    }

    Ok(())
}
