use faststr::FastStr;
use serde::{Deserialize, Serialize};

use crate::{
    storage::{ReplicaPlacement, Ttl, VolumeError},
    topology::volume_grow::VolumeGrowOption,
};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Assignment {
    pub fid: String,
    pub url: String,
    pub public_url: FastStr,
    pub count: u64,
    pub error: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssignRequest {
    pub count: Option<u64>,
    pub replication: Option<FastStr>,
    pub ttl: Option<FastStr>,
    pub preallocate: Option<i64>,
    pub collection: Option<FastStr>,
    pub data_center: Option<FastStr>,
    pub rack: Option<FastStr>,
    pub data_node: Option<FastStr>,
}

impl AssignRequest {
    pub fn volume_grow_option(
        self,
        default_replication: &FastStr,
    ) -> Result<VolumeGrowOption, VolumeError> {
        let mut option = VolumeGrowOption::default();
        match self.replication {
            Some(mut replication) => {
                if replication.is_empty() {
                    replication = default_replication.clone();
                }
                option.replica_placement = ReplicaPlacement::new(&replication)?;
            }
            None => {
                option.replica_placement = ReplicaPlacement::new(default_replication)?;
            }
        }
        if let Some(ttl) = self.ttl {
            option.ttl = Ttl::new(&ttl)?;
        }
        if let Some(preallocate) = self.preallocate {
            option.preallocate = preallocate;
        }
        if let Some(collection) = self.collection {
            option.collection = collection;
        }
        if let Some(data_center) = self.data_center {
            option.data_center = data_center;
        }
        if let Some(rack) = self.rack {
            option.rack = rack;
        }
        if let Some(data_node) = self.data_node {
            option.data_node = data_node;
        }
        Ok(option)
    }
}
