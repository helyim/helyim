use std::{
    collections::BTreeMap,
    io::{Error, ErrorKind},
    time::Duration,
};

use faststr::FastStr;
use serde::{Deserialize, Serialize};

use crate::http::{HttpError, HTTP_CLIENT};

#[derive(Serialize, Deserialize)]
pub struct ClusterStatus {
    pub is_leader: bool,
    pub leader: FastStr,
    pub peers: BTreeMap<u64, FastStr>,
}

pub async fn list_master(addr: &str) -> Result<ClusterStatus, HttpError> {
    for _ in 0..3 {
        let cluster_status: ClusterStatus = HTTP_CLIENT
            .get(format!("http://{addr}/cluster/status"))
            .send()
            .await?
            .json()
            .await?;
        if !cluster_status.peers.is_empty() {
            return Ok(cluster_status);
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Err(Error::new(
        ErrorKind::AddrNotAvailable,
        format!("list master: {addr} is not available"),
    ))?
}
