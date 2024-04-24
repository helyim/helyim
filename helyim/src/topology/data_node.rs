use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    result::Result as StdResult,
    sync::{atomic::AtomicU64, Arc, Weak},
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
use tokio::sync::RwLock;

use crate::{
    storage::{erasure_coding::EcVolumeInfo, VolumeError, VolumeId, VolumeInfo},
    topology::{node::Node, rack::Rack},
    util::grpc::volume_server_client,
};

#[derive(Serialize)]
pub struct DataNode {
    pub ip: FastStr,
    pub port: u16,
    pub public_url: FastStr,
    pub last_seen: i64,
    node: Node,
    #[serde(skip)]
    pub rack: RwLock<Weak<Rack>>,
    pub volumes: DashMap<VolumeId, VolumeInfo>,
    pub ec_shards: DashMap<VolumeId, EcVolumeInfo>,
    pub ec_shard_count: AtomicU64,
}

impl std::fmt::Display for DataNode {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
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
        let node = Node::new(id);
        node.set_max_volume_count(max_volume_count);

        DataNode {
            ip,
            port,
            public_url,
            last_seen: 0,
            node,
            rack: RwLock::new(Weak::new()),
            volumes: DashMap::new(),
            ec_shards: DashMap::new(),
            ec_shard_count: AtomicU64::new(0),
        }
    }

    pub fn url(&self) -> String {
        format!("{}:{}", self.ip, self.port)
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
        match self.rack.read().await.upgrade() {
            Some(rack) => rack.id.clone(),
            None => FastStr::empty(),
        }
    }
    pub async fn data_center_id(&self) -> FastStr {
        match self.rack.read().await.upgrade() {
            Some(rack) => rack.data_center_id().await,
            None => FastStr::empty(),
        }
    }
    pub async fn set_rack(&self, rack: Weak<Rack>) {
        *self.rack.write().await = rack;
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

impl DataNode {
    /// number of free volume slots
    pub fn free_space(&self) -> i64 {
        self.max_volume_count() - self.volume_count()
    }

    pub fn volume_count(&self) -> i64 {
        self._volume_count()
    }

    pub fn max_volume_count(&self) -> i64 {
        self._max_volume_count()
    }

    pub async fn adjust_volume_count(&self, volume_count_delta: i64) {
        self._adjust_volume_count(volume_count_delta);

        if let Some(rack) = self.rack.read().await.upgrade() {
            rack.adjust_volume_count(volume_count_delta).await;
        }
    }

    pub async fn adjust_active_volume_count(&self, active_volume_count_delta: i64) {
        self._adjust_active_volume_count(active_volume_count_delta);

        if let Some(rack) = self.rack.read().await.upgrade() {
            rack.adjust_active_volume_count(active_volume_count_delta)
                .await;
        }
    }

    pub async fn adjust_ec_shard_count(&self, ec_shard_count_delta: i64) {
        self._adjust_ec_shard_count(ec_shard_count_delta);

        if let Some(rack) = self.rack.read().await.upgrade() {
            rack.adjust_ec_shard_count(ec_shard_count_delta).await;
        }
    }

    pub async fn adjust_max_volume_count(&self, max_volume_count_delta: i64) {
        self._adjust_max_volume_count(max_volume_count_delta);

        if let Some(rack) = self.rack.read().await.upgrade() {
            rack.adjust_max_volume_count(max_volume_count_delta).await;
        }
    }

    pub async fn adjust_max_volume_id(&self, vid: VolumeId) {
        self._adjust_max_volume_id(vid);

        if let Some(rack) = self.rack.read().await.upgrade() {
            rack.adjust_max_volume_id(self.max_volume_id()).await;
        }
    }
}

impl Deref for DataNode {
    type Target = Node;

    fn deref(&self) -> &Self::Target {
        &self.node
    }
}

impl DerefMut for DataNode {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.node
    }
}

pub type DataNodeRef = Arc<DataNode>;

#[cfg(test)]
mod test {
    use faststr::FastStr;

    use crate::{
        storage::{ReplicaPlacement, Ttl, VolumeInfo, CURRENT_VERSION},
        topology::data_node::DataNode,
    };

    fn setup() -> DataNode {
        let id = FastStr::new("127.0.0.1:9333");
        let ip = FastStr::new("127.0.0.1");
        let public_url = id.clone();
        DataNode::new(id, ip, 9333, public_url, 1)
    }

    #[tokio::test]
    pub async fn test_add_or_update_volume() {
        let data_node = setup();
        let volume = VolumeInfo {
            id: 0,
            size: 0,
            collection: FastStr::new("default_collection"),
            version: CURRENT_VERSION,
            ttl: Ttl::new("1d").unwrap(),
            replica_placement: ReplicaPlacement::new("000").unwrap(),
            ..Default::default()
        };
        let is_new = data_node.add_or_update_volume(&volume).await;
        assert!(is_new);
        let is_new = data_node.add_or_update_volume(&volume).await;
        assert!(!is_new);

        assert_eq!(data_node.max_volume_id(), 0);
        assert_eq!(data_node.max_volume_count(), 1);
        assert_eq!(data_node.active_volume_count(), 1);
        assert_eq!(data_node.free_space(), 0);
    }
}
