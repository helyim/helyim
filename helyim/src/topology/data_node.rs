use std::{
    collections::HashMap,
    fmt::{Debug, Display, Formatter},
    result::Result as StdResult,
    sync::{atomic::AtomicU64, Arc},
};

use dashmap::{mapref::one::Ref, DashMap};
use faststr::FastStr;
use helyim_proto::volume::{
    AllocateVolumeRequest, AllocateVolumeResponse, VacuumVolumeCheckRequest,
    VacuumVolumeCheckResponse, VacuumVolumeCleanupRequest, VacuumVolumeCleanupResponse,
    VacuumVolumeCommitRequest, VacuumVolumeCommitResponse, VacuumVolumeCompactRequest,
    VacuumVolumeCompactResponse,
};
use serde::Serialize;

use crate::{
    storage::{erasure_coding::EcVolumeInfo, VolumeError, VolumeId, VolumeInfo},
    topology::node::{Node, NodeImpl, NodeType},
    util::grpc::volume_server_client,
};

#[derive(Serialize)]
pub struct DataNode {
    pub ip: FastStr,
    pub port: u16,
    pub url: FastStr,
    pub public_url: FastStr,
    pub last_seen: i64,
    node: Arc<NodeImpl>,

    pub volumes: DashMap<VolumeId, VolumeInfo>,
    pub ec_shards: DashMap<VolumeId, EcVolumeInfo>,
    pub ec_shard_count: AtomicU64,
}

impl Debug for DataNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DataNode ({}:{})", self.ip, self.port)
    }
}

impl Display for DataNode {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DataNode({})", self.id())
    }
}

impl DataNode {
    pub fn new(
        id: FastStr,
        ip: FastStr,
        port: u16,
        public_url: FastStr,
        max_volume_count: i64,
    ) -> DataNode {
        let url = format!("{id}:{port}");
        let node = Arc::new(NodeImpl::new(id));
        node.set_max_volume_count(max_volume_count);

        let url = FastStr::new(url);
        DataNode {
            ip,
            port,
            url,
            public_url,
            last_seen: 0,
            node,
            volumes: DashMap::new(),
            ec_shards: DashMap::new(),
            ec_shard_count: AtomicU64::new(0),
        }
    }

    pub fn url(&self) -> FastStr {
        self.url.clone()
    }

    pub async fn delta_update_volumes(
        &self,
        new_volumes: &[VolumeInfo],
        deleted_volumes: &[VolumeInfo],
    ) {
        for volume in deleted_volumes {
            if self.volumes.remove(&volume.id).is_some() {
                self.adjust_volume_count(-1).await;

                if !volume.read_only {
                    self.adjust_active_volume_count(-1).await;
                }
            }
        }

        for volume in new_volumes {
            self.add_or_update_volume(volume).await;
        }
    }

    pub async fn update_volumes(
        &self,
        volume_infos: Vec<VolumeInfo>,
    ) -> (Vec<VolumeInfo>, Vec<VolumeInfo>) {
        let mut actual_volume_map = HashMap::new();
        for info in volume_infos.iter() {
            actual_volume_map.insert(info.id, info);
        }

        let mut deleted_id = vec![];
        let mut deleted_volumes = vec![];
        let mut new_volumes = vec![];

        for volume in self.volumes.iter() {
            if !actual_volume_map.contains_key(volume.key()) {
                deleted_id.push(volume.id);
            }
        }

        for info in volume_infos {
            if self.add_or_update_volume(&info).await {
                new_volumes.push(info);
            }
        }

        for id in deleted_id.iter() {
            if let Some((_, volume)) = self.volumes.remove(id) {
                self.adjust_volume_count(-1).await;
                if !volume.read_only {
                    self.adjust_active_volume_count(-1).await;
                }
                deleted_volumes.push(volume);
            }
        }

        (new_volumes, deleted_volumes)
    }

    #[allow(clippy::map_entry)]
    pub async fn add_or_update_volume(&self, v: &VolumeInfo) -> bool {
        let mut is_new = false;
        if self.volumes.contains_key(&v.id) {
            self.volumes.insert(v.id, v.clone());
        } else {
            self.adjust_volume_count(1).await;
            if !v.read_only {
                self.adjust_active_volume_count(1).await;
            }
            self.adjust_max_volume_id(v.id).await;
            self.volumes.insert(v.id, v.clone());
            is_new = true
        }

        is_new
    }

    pub fn get_volume(&self, vid: VolumeId) -> Option<Ref<VolumeId, VolumeInfo>> {
        self.volumes.get(&vid)
    }

    pub async fn rack_id(&self) -> FastStr {
        match self.parent().await {
            Some(rack) => FastStr::new(rack.id()),
            None => FastStr::empty(),
        }
    }
    pub async fn data_center_id(&self) -> FastStr {
        if let Some(rack) = self.parent().await {
            if let Some(data_center) = rack.parent().await {
                return FastStr::new(data_center.id());
            }
        }
        FastStr::empty()
    }
}

impl DataNode {
    pub async fn allocate_volume(
        &self,
        request: AllocateVolumeRequest,
    ) -> StdResult<AllocateVolumeResponse, VolumeError> {
        let addr = self.url();
        let client = volume_server_client(&addr)?;
        let response = client.allocate_volume(request).await?;
        Ok(response.into_inner())
    }

    pub async fn vacuum_volume_check(
        &self,
        request: VacuumVolumeCheckRequest,
    ) -> StdResult<VacuumVolumeCheckResponse, VolumeError> {
        let addr = self.url();
        let client = volume_server_client(&addr)?;
        let response = client.vacuum_volume_check(request).await?;
        Ok(response.into_inner())
    }

    pub async fn vacuum_volume_compact(
        &self,
        request: VacuumVolumeCompactRequest,
    ) -> StdResult<VacuumVolumeCompactResponse, VolumeError> {
        let addr = self.url();
        let client = volume_server_client(&addr)?;
        let response = client.vacuum_volume_compact(request).await?;
        Ok(response.into_inner())
    }

    pub async fn vacuum_volume_commit(
        &self,
        request: VacuumVolumeCommitRequest,
    ) -> StdResult<VacuumVolumeCommitResponse, VolumeError> {
        let addr = self.url();
        let client = volume_server_client(&addr)?;
        let response = client.vacuum_volume_commit(request).await?;
        Ok(response.into_inner())
    }

    pub async fn vacuum_volume_cleanup(
        &self,
        request: VacuumVolumeCleanupRequest,
    ) -> StdResult<VacuumVolumeCleanupResponse, VolumeError> {
        let addr = self.url();
        let client = volume_server_client(&addr)?;
        let response = client.vacuum_volume_cleanup(request).await?;
        Ok(response.into_inner())
    }
}

impl_node!(DataNode);

pub type DataNodeRef = Arc<DataNode>;

#[cfg(test)]
mod test {
    use faststr::FastStr;

    use crate::{
        storage::{ReplicaPlacement, Ttl, VolumeId, VolumeInfo, CURRENT_VERSION},
        topology::{data_node::DataNode, node::Node},
    };

    fn setup() -> DataNode {
        let id = FastStr::new("127.0.0.1:8080");
        let ip = FastStr::new("127.0.0.1");
        let public_url = id.clone();
        DataNode::new(id, ip, 8080, public_url, 1)
    }

    fn volume_info(id: VolumeId) -> VolumeInfo {
        VolumeInfo {
            id,
            size: 0,
            collection: FastStr::new("default_collection"),
            version: CURRENT_VERSION,
            ttl: Ttl::new("1d").unwrap(),
            replica_placement: ReplicaPlacement::new("000").unwrap(),
            read_only: false,
            ..Default::default()
        }
    }

    #[tokio::test]
    pub async fn test_add_or_update_volume() {
        let data_node = setup();
        let volume = volume_info(0);
        let is_new = data_node.add_or_update_volume(&volume).await;
        assert!(is_new);
        let is_new = data_node.add_or_update_volume(&volume).await;
        assert!(!is_new);

        assert_eq!(data_node.max_volume_id(), 0);
        assert_eq!(data_node.max_volume_count(), 1);
        assert_eq!(data_node.active_volume_count(), 1);
        assert_eq!(data_node.free_space(), 0);
    }

    #[tokio::test]
    pub async fn test_delta_update_volume() {
        let data_node = setup();
        let new_volumes = vec![volume_info(0), volume_info(1), volume_info(2)];
        let deleted_volumes = vec![];

        data_node
            .delta_update_volumes(&new_volumes, &deleted_volumes)
            .await;
        assert_eq!(data_node.volumes.len(), 3);
        assert_eq!(data_node.volume_count(), 3);
        assert_eq!(data_node.active_volume_count(), 3);

        data_node
            .delta_update_volumes(&deleted_volumes, &new_volumes)
            .await;
        assert_eq!(data_node.volumes.len(), 0);
        assert_eq!(data_node.volume_count(), 0);
        assert_eq!(data_node.active_volume_count(), 0);
    }

    #[tokio::test]
    pub async fn test_update_volume() {
        let data_node = setup();
        let update_volumes = vec![volume_info(0), volume_info(1), volume_info(2)];

        let (new_volumes, deleted_volumes) = data_node.update_volumes(update_volumes).await;
        assert_eq!(data_node.volumes.len(), 3);
        assert_eq!(data_node.volume_count(), 3);
        assert_eq!(data_node.active_volume_count(), 3);
        assert_eq!(new_volumes.len(), 3);
        assert_eq!(deleted_volumes.len(), 0);

        let update_volumes = vec![volume_info(0), volume_info(2)];
        let (new_volumes, deleted_volumes) = data_node.update_volumes(update_volumes).await;
        assert_eq!(data_node.volumes.len(), 2);
        assert_eq!(data_node.volume_count(), 2);
        assert_eq!(data_node.active_volume_count(), 2);
        assert_eq!(new_volumes.len(), 0);
        assert_eq!(deleted_volumes.len(), 1);
    }
}
