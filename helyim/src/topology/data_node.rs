use std::{
    collections::HashSet,
    ops::{Deref, DerefMut},
    result::Result as StdResult,
    sync::{atomic::AtomicU64, Arc, Weak},
};

use dashmap::DashMap;
use faststr::FastStr;
use helyim_proto::{
    AllocateVolumeRequest, AllocateVolumeResponse, VacuumVolumeCheckRequest,
    VacuumVolumeCheckResponse, VacuumVolumeCleanupRequest, VacuumVolumeCleanupResponse,
    VacuumVolumeCommitRequest, VacuumVolumeCommitResponse, VacuumVolumeCompactRequest,
    VacuumVolumeCompactResponse,
};
use serde::Serialize;
use tokio::sync::RwLock;

use crate::{
    errors::Result,
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
    volumes: DashMap<VolumeId, VolumeInfo>,
    pub ec_shards: DashMap<VolumeId, EcVolumeInfo>,
    pub ec_shard_count: AtomicU64,
}

impl std::fmt::Display for DataNode {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DataNode({})", self.id())
    }
}

impl DataNode {
    pub async fn new(
        id: FastStr,
        ip: FastStr,
        port: u16,
        public_url: FastStr,
        max_volume_count: u64,
    ) -> Result<DataNode> {
        let node = Node::new(id);
        node.set_max_volume_count(max_volume_count);

        Ok(DataNode {
            ip,
            port,
            public_url,
            last_seen: 0,
            node,
            rack: RwLock::new(Weak::new()),
            volumes: DashMap::new(),
            ec_shards: DashMap::new(),
            ec_shard_count: AtomicU64::new(0),
        })
    }

    pub fn url(&self) -> String {
        format!("{}:{}", self.ip, self.port)
    }

    pub async fn update_volumes(&self, volume_infos: Vec<VolumeInfo>) -> Vec<VolumeInfo> {
        let mut volumes = HashSet::new();
        for info in volume_infos.iter() {
            volumes.insert(info.id);
        }

        let mut deleted_id = vec![];
        let mut deleted = vec![];

        for volume in self.volumes.iter() {
            if !volumes.contains(volume.key()) {
                deleted_id.push(volume.id);

                self.adjust_volume_count(-1).await;
                if !volume.read_only {
                    self.adjust_active_volume_count(-1).await;
                }
            }
        }

        for info in volume_infos {
            self.add_or_update_volume(info).await;
        }

        for id in deleted_id.iter() {
            if let Some((_, volume)) = self.volumes.remove(id) {
                deleted.push(volume);
            }
        }

        deleted
    }

    #[allow(clippy::map_entry)]
    pub async fn add_or_update_volume(&self, v: VolumeInfo) {
        if self.volumes.contains_key(&v.id) {
            self.volumes.insert(v.id, v);
        } else {
            self.adjust_volume_count(1).await;
            if !v.read_only {
                self.adjust_active_volume_count(1).await;
            }
            self.adjust_max_volume_id(v.id).await;
            self.volumes.insert(v.id, v);
        }
    }

    pub fn get_volume(&self, vid: VolumeId) -> Option<VolumeInfo> {
        self.volumes.get(&vid).map(|volume| volume.value().clone())
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

    pub async fn allocate_volume(
        &self,
        request: AllocateVolumeRequest,
    ) -> StdResult<AllocateVolumeResponse, VolumeError> {
        let addr = self.url();
        let client = volume_server_client(&addr)?;
        let response = client.allocate_volume(request.clone()).await?;
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
    pub fn free_volumes(&self) -> u64 {
        self.max_volume_count() - self.volume_count()
    }

    pub fn volume_count(&self) -> u64 {
        self._volume_count()
    }

    pub fn max_volume_count(&self) -> u64 {
        self._max_volume_count()
    }

    pub async fn adjust_volume_count(&self, volume_count_delta: i64) {
        self._adjust_volume_count(volume_count_delta);

        if let Some(rack) = self.rack.read().upgrade() {
            rack.adjust_volume_count(volume_count_delta).await;
        }
    }

    pub async fn adjust_active_volume_count(&self, active_volume_count_delta: i64) {
        self._adjust_active_volume_count(active_volume_count_delta);

        if let Some(rack) = self.rack.read().upgrade() {
            rack.adjust_active_volume_count(active_volume_count_delta)
                .await;
        }
    }

    pub async fn adjust_ec_shard_count(&self, ec_shard_count_delta: i64) {
        self._adjust_ec_shard_count(ec_shard_count_delta);

        if let Some(rack) = self.rack.read().upgrade() {
            rack.adjust_ec_shard_count(ec_shard_count_delta).await;
        }
    }

    pub async fn adjust_max_volume_count(&self, max_volume_count_delta: i64) {
        self._adjust_max_volume_count(max_volume_count_delta);

        if let Some(rack) = self.rack.read().upgrade() {
            rack.adjust_max_volume_count(max_volume_count_delta).await;
        }
    }

    pub async fn adjust_max_volume_id(&self, vid: VolumeId) {
        self._adjust_max_volume_id(vid);

        if let Some(rack) = self.rack.read().upgrade() {
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
