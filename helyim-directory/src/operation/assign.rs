use faststr::FastStr;
use helyim_client::helyim_client;
use helyim_common::{ttl::Ttl, types::ReplicaPlacement};
use helyim_proto::directory::AssignRequest as PbAssignRequest;
use helyim_topology::{volume_grow::VolumeGrowOption, TopologyError};
use serde::{Deserialize, Serialize};
use tonic::Status;

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

pub async fn assign(server: &str, request: AssignRequest) -> Result<Assignment, Status> {
    let client = helyim_client(server)?;

    let mut writable_volume_count = request.writable_volume_count.unwrap_or_default();
    if writable_volume_count < 0 {
        writable_volume_count = 0;
    }
    let request = PbAssignRequest {
        count: request.count.unwrap_or_default() as u64,
        replication: request.replication.unwrap_or_default().to_string(),
        collection: request.collection.unwrap_or_default().to_string(),
        ttl: request.ttl.unwrap_or_default().to_string(),
        data_center: request.data_center.unwrap_or_default().to_string(),
        rack: request.rack.unwrap_or_default().to_string(),
        data_node: request.data_node.unwrap_or_default().to_string(),
        writable_volume_count: writable_volume_count as u32,
        // FIXME: what values should they be set to?
        memory_map_max_size_mb: u32::MAX,
    };

    let response = client.assign(request).await?;
    let response = response.into_inner();
    Ok(Assignment {
        fid: FastStr::new(response.fid),
        url: FastStr::new(response.url),
        public_url: FastStr::new(response.public_url),
        count: response.count,
        error: FastStr::new(response.error),
    })
}
