use std::sync::atomic::Ordering;

use crate::topology::data_center::DataCenter;

impl DataCenter {
    pub async fn up_adjust_ec_shard_count_delta(&self, ec_shard_count_delta: u64) {
        self.ec_shard_count
            .fetch_add(ec_shard_count_delta, Ordering::Relaxed);
        if let Some(topology) = self.topology.upgrade() {
            topology
                .write()
                .await
                .up_adjust_ec_shard_count_delta(ec_shard_count_delta)
                .await;
        }
    }
}
