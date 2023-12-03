use std::sync::atomic::Ordering;

use crate::topology::rack::Rack;

impl Rack {
    pub async fn up_adjust_ec_shard_count_delta(&self, ec_shard_count_delta: u64) {
        self.ec_shard_count
            .fetch_add(ec_shard_count_delta, Ordering::Relaxed);
        if let Some(data_center) = self.data_center.upgrade() {
            data_center
                .write()
                .await
                .up_adjust_ec_shard_count_delta(ec_shard_count_delta)
                .await;
        }
    }
}
