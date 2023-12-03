use std::{collections::HashMap, sync::atomic::Ordering};

use crate::{storage::erasure_coding::EcVolumeInfo, topology::data_node::DataNode};

impl DataNode {
    pub async fn up_adjust_ec_shard_count_delta(&self, ec_shard_count_delta: u64) {
        self.ec_shard_count
            .fetch_add(ec_shard_count_delta, Ordering::Relaxed);
        if let Some(rack) = self.rack.upgrade() {
            rack.write()
                .await
                .up_adjust_ec_shard_count_delta(ec_shard_count_delta)
                .await;
        }
    }

    pub fn get_ec_shards(&self) -> Vec<&EcVolumeInfo> {
        let mut volumes = Vec::new();
        for (_, ec_volume) in self.ec_shards.iter() {
            volumes.push(ec_volume);
        }
        volumes
    }

    pub async fn update_ec_shards(
        &mut self,
        actual_shards: &mut [EcVolumeInfo],
    ) -> (Vec<EcVolumeInfo>, Vec<EcVolumeInfo>) {
        // prepare the new ec shards map
        let mut actual_ec_shard_map = HashMap::new();
        for ec_shards in actual_shards.iter() {
            actual_ec_shard_map.insert(ec_shards.volume_id, ec_shards.clone());
        }

        // found out the new shards and deleted shards
        let mut new_shard_count = 0;
        let mut deleted_shard_count = 0;

        let mut new_shards = Vec::new();
        let mut deleted_shards = Vec::new();
        for (vid, ec_shards) in self.ec_shards.iter_mut() {
            match actual_ec_shard_map.get(vid) {
                Some(actual_ec_shards) => {
                    let mut a = actual_ec_shards.minus(ec_shards);
                    if a.shard_bits.shard_id_count() > 0 {
                        new_shard_count += a.shard_bits.shard_id_count();
                        new_shards.push(a);
                    }
                    let mut d = ec_shards.minus(actual_ec_shards);
                    if d.shard_bits.shard_id_count() > 0 {
                        deleted_shard_count += d.shard_bits.shard_id_count();
                        deleted_shards.push(d);
                    }
                }
                None => {
                    deleted_shards.push(ec_shards.clone());
                    deleted_shard_count += ec_shards.shard_bits.shard_id_count();
                }
            }
        }

        for ec_shards in actual_shards.iter_mut() {
            if self.ec_shards.contains_key(&ec_shards.volume_id) {
                new_shards.push(ec_shards.clone());
                new_shard_count += ec_shards.shard_bits.shard_id_count();
            }
        }

        if !new_shards.is_empty() || !deleted_shards.is_empty() {
            self.ec_shards = actual_ec_shard_map;
            self.up_adjust_ec_shard_count_delta(new_shard_count - deleted_shard_count)
                .await;
        }

        (new_shards, deleted_shards)
    }

    pub async fn delta_update_ec_shards(
        &mut self,
        new_shards: &mut [EcVolumeInfo],
        deleted_shards: &mut [EcVolumeInfo],
    ) {
        for shard in new_shards.iter_mut() {
            self.add_or_update_ec_shards(shard).await;
        }
        for shard in deleted_shards.iter_mut() {
            self.delete_ec_shard(shard).await;
        }
    }

    pub async fn add_or_update_ec_shards(&mut self, volume: &mut EcVolumeInfo) {
        let delta = match self.ec_shards.get_mut(&volume.volume_id) {
            Some(ec_shard) => {
                let old_count = ec_shard.shard_bits.shard_id_count();
                ec_shard.shard_bits = ec_shard.shard_bits.plus(volume.shard_bits);
                ec_shard.shard_bits.shard_id_count() - old_count
            }
            None => {
                let delta = volume.shard_bits.shard_id_count();
                self.ec_shards.insert(volume.volume_id, volume.clone());
                delta
            }
        };
        self.up_adjust_ec_shard_count_delta(delta).await;
    }

    pub async fn delete_ec_shard(&mut self, volume: &mut EcVolumeInfo) {
        if let Some(ec_shard) = self.ec_shards.get_mut(&volume.volume_id) {
            let old_count = ec_shard.shard_bits.shard_id_count();
            ec_shard.shard_bits = ec_shard.shard_bits.minus(volume.shard_bits);
            let delta = ec_shard.shard_bits.shard_id_count() - old_count;
            if ec_shard.shard_bits.shard_id_count() == 0 {
                self.ec_shards.remove(&volume.volume_id);
            }
            self.up_adjust_ec_shard_count_delta(delta).await;
        }
    }
}
