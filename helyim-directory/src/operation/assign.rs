use faststr::FastStr;
use helyim_common::{ttl::Ttl, types::ReplicaPlacement};
use helyim_topology::{volume_grow::VolumeGrowOption, TopologyError};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Assignment {
    pub fid: FastStr,
    pub url: FastStr,
    pub public_url: FastStr,
    pub count: u64,
    pub error: FastStr,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssignRequest {
    pub count: Option<i64>,
    pub replication: Option<FastStr>,
    pub ttl: Option<FastStr>,
    pub preallocate: Option<i64>,
    pub collection: Option<FastStr>,
    pub data_center: Option<FastStr>,
    pub rack: Option<FastStr>,
    pub data_node: Option<FastStr>,
    pub writable_volume_count: Option<i32>,
}

impl AssignRequest {
    pub fn volume_grow_option(
        self,
        default_replication: &str,
    ) -> Result<VolumeGrowOption, TopologyError> {
        let mut option = VolumeGrowOption::default();
        match self.replication {
            Some(replication) => {
                let replication = if replication.is_empty() {
                    default_replication
                } else {
                    &replication
                };
                option.replica_placement = ReplicaPlacement::new(replication)?;
            }
            None => {
                option.replica_placement = ReplicaPlacement::new(default_replication)?;
            }
        }
        if let Some(ttl) = self.ttl {
            option.ttl = Ttl::new(&ttl)?;
        }
        if let Some(preallocate) = self.preallocate {
            option.preallocate = preallocate as u64;
        }
        if let Some(collection) = self.collection {
            option.collection = collection.clone();
        }
        if let Some(data_center) = self.data_center {
            option.data_center = data_center.clone();
        }
        if let Some(rack) = self.rack {
            option.rack = rack.clone();
        }
        if let Some(data_node) = self.data_node {
            option.data_node = data_node.clone();
        }
        Ok(option)
    }
}
