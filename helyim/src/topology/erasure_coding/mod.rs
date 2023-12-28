#![allow(dead_code)]

use faststr::FastStr;
use helyim_proto::VolumeEcShardInformationMessage;

use crate::{
    storage::erasure_coding::{EcVolumeInfo, ShardId, TOTAL_SHARDS_COUNT},
    topology::{DataNodeRef, Topology},
};

mod data_node;

#[derive(Clone)]
pub struct EcShardLocations {
    collection: FastStr,
    locations: Vec<Vec<DataNodeRef>>,
}

impl EcShardLocations {
    pub fn new(collection: FastStr) -> Self {
        Self {
            collection,
            locations: vec![vec![]; TOTAL_SHARDS_COUNT as usize],
        }
    }

    pub async fn add_shard(&mut self, shard_id: ShardId, data_node: DataNodeRef) -> bool {
        let data_nodes = &self.locations[shard_id as usize];
        for node in data_nodes {
            if node.id() == data_node.id() {
                return false;
            }
        }
        self.locations[shard_id as usize].push(data_node);
        true
    }

    pub async fn delete_shard(&mut self, shard_id: ShardId, data_node: &DataNodeRef) -> bool {
        let data_nodes = &self.locations[shard_id as usize];
        let mut index = -1;
        for (i, node) in data_nodes.iter().enumerate() {
            if node.id() == data_node.id() {
                index = i as i32;
                break;
            }
        }
        if index < 0 {
            return false;
        }
        self.locations[shard_id as usize].remove(index as usize);
        true
    }
}

impl Topology {
    pub async fn sync_data_node_ecshards(
        &mut self,
        shard_infos: &[VolumeEcShardInformationMessage],
        data_node: DataNodeRef,
    ) -> (Vec<EcVolumeInfo>, Vec<EcVolumeInfo>) {
        let mut shards = Vec::new();
        for shard in shard_infos {
            let shard = EcVolumeInfo::new(
                FastStr::new(shard.collection.clone()),
                shard.id,
                shard.ec_index_bits.into(),
            );
            shards.push(shard);
        }
        let (new_shards, deleted_shards) = data_node.update_ec_shards(&mut shards).await;
        for shard in new_shards.iter() {
            self.register_ec_shards(shard, &data_node).await;
        }
        for shard in deleted_shards.iter() {
            self.unregister_ec_shards(shard, &data_node).await;
        }
        (new_shards, deleted_shards)
    }

    pub async fn increment_sync_data_node_ec_shards(
        &mut self,
        new_ec_shards: Vec<VolumeEcShardInformationMessage>,
        deleted_ec_shards: Vec<VolumeEcShardInformationMessage>,
        data_node: DataNodeRef,
    ) {
        let mut new_shards = Vec::new();
        let mut deleted_shards = Vec::new();

        for shard in new_ec_shards.iter() {
            new_shards.push(EcVolumeInfo::new(
                FastStr::new(shard.collection.clone()),
                shard.id,
                shard.ec_index_bits.into(),
            ));
        }

        for shard in deleted_ec_shards.iter() {
            deleted_shards.push(EcVolumeInfo::new(
                FastStr::new(shard.collection.clone()),
                shard.id,
                shard.ec_index_bits.into(),
            ));
        }

        data_node
            .delta_update_ec_shards(&mut new_shards, &mut deleted_shards)
            .await;

        for shard in new_shards.iter() {
            self.register_ec_shards(shard, &data_node).await;
        }
        for shard in deleted_shards.iter() {
            self.unregister_ec_shards(shard, &data_node).await;
        }
    }

    pub async fn register_ec_shards(
        &mut self,
        ec_shard_infos: &EcVolumeInfo,
        data_node: &DataNodeRef,
    ) {
        match self.ec_shards.get_mut(&ec_shard_infos.volume_id) {
            Some(mut locations) => {
                for shard_id in ec_shard_infos.shard_bits.shard_ids() {
                    let _ = locations.add_shard(shard_id, data_node.clone()).await;
                }
            }
            None => {
                let mut locations = EcShardLocations::new(ec_shard_infos.collection.clone());
                for shard_id in ec_shard_infos.shard_bits.shard_ids() {
                    let _ = locations.add_shard(shard_id, data_node.clone()).await;
                }
                self.ec_shards.insert(ec_shard_infos.volume_id, locations);
            }
        }
    }

    pub async fn unregister_ec_shards(
        &mut self,
        ec_shard_infos: &EcVolumeInfo,
        data_node: &DataNodeRef,
    ) {
        if let Some(mut locations) = self.ec_shards.get_mut(&ec_shard_infos.volume_id) {
            for shard_id in ec_shard_infos.shard_bits.shard_ids() {
                let _ = locations.delete_shard(shard_id, data_node).await;
            }
        }
    }
}
