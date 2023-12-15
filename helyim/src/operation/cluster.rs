use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::raft::types::NodeId;

#[derive(Serialize, Deserialize)]
pub struct ClusterStatus {
    pub is_leader: bool,
    pub leader: String,
    pub peers: BTreeMap<NodeId, String>,
}
