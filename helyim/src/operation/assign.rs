use faststr::FastStr;
use helyim_proto::directory::AssignRequest as PbAssignRequest;
use serde::{Deserialize, Serialize};

use crate::{
    storage::{ReplicaPlacement, Ttl, VolumeError},
    topology::volume_grow::VolumeGrowOption,
    util::grpc::helyim_client,
};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Assignment {
    pub fid: String,
    pub url: String,
    pub public_url: String,
    pub count: u64,
    pub error: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssignRequest {
    pub count: Option<u64>,
    pub replication: Option<String>,
    pub ttl: Option<String>,
    pub preallocate: Option<i64>,
    pub collection: Option<String>,
    pub data_center: Option<String>,
    pub rack: Option<String>,
    pub data_node: Option<String>,
}

impl AssignRequest {
    pub fn volume_grow_option(
        self,
        default_replication: &str,
    ) -> Result<VolumeGrowOption, VolumeError> {
        let mut option = VolumeGrowOption::default();
        match self.replication {
            Some(mut replication) => {
                if replication.is_empty() {
                    replication = default_replication.to_string();
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
            option.preallocate = preallocate as u64;
        }
        if let Some(collection) = self.collection {
            option.collection = FastStr::new(collection);
        }
        if let Some(data_center) = self.data_center {
            option.data_center = FastStr::new(data_center);
        }
        if let Some(rack) = self.rack {
            option.rack = FastStr::new(rack);
        }
        if let Some(data_node) = self.data_node {
            option.data_node = FastStr::new(data_node);
        }
        Ok(option)
    }
}

pub async fn assign(server: &str, request: AssignRequest) -> Result<Assignment, VolumeError> {
    let client = helyim_client(server)?;
    let request = PbAssignRequest {
        count: request.count.unwrap_or_default(),
        replication: request.replication.unwrap_or_default(),
        collection: request.collection.unwrap_or_default(),
        ttl: request.ttl.unwrap_or_default(),
        data_center: request.data_center.unwrap_or_default(),
        rack: request.rack.unwrap_or_default(),
        data_node: request.data_node.unwrap_or_default(),
        // FIXME: what values should they be set to?
        memory_map_max_size_mb: u32::MAX,
        writable_volume_count: 1,
    };

    let response = client.assign(request).await?;
    let response = response.into_inner();
    Ok(Assignment {
        fid: response.fid,
        url: response.url,
        public_url: response.public_url,
        count: response.count,
        error: response.error,
    })
}
