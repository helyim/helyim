use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct ClusterStatus {
    pub is_leader: bool,
    pub leader: String,
    pub peers: Vec<String>,
}
