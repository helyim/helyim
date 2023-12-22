use faststr::FastStr;
use serde::{Deserialize, Serialize};

use crate::util::http::HTTP_CLIENT;

#[derive(Serialize, Deserialize)]
pub struct ClusterStatus {
    pub is_leader: bool,
    pub leader: FastStr,
    pub peers: Vec<FastStr>,
}

pub async fn list_master(addr: &str) -> Result<ClusterStatus, reqwest::Error> {
    HTTP_CLIENT
        .get(format!("http://{addr}/cluster/status"))
        .send()
        .await?
        .json()
        .await
}
