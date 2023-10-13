use faststr::FastStr;

use crate::{storage::erasure_coding::TOTAL_SHARDS_COUNT, topology::DataNodeEventTx};

#[derive(Clone)]
pub struct EcShardLocations {
    collection: FastStr,
    locations: [Vec<DataNodeEventTx>; TOTAL_SHARDS_COUNT as usize],
}
