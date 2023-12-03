use std::{
    collections::{HashMap, HashSet},
    result::Result as StdResult,
    sync::{atomic::AtomicU64, Arc},
};

use faststr::FastStr;
use ginepro::LoadBalancedChannel;
use helyim_proto::{
    volume_server_client::VolumeServerClient, AllocateVolumeRequest, AllocateVolumeResponse,
    VacuumVolumeCheckRequest, VacuumVolumeCheckResponse, VacuumVolumeCleanupRequest,
    VacuumVolumeCleanupResponse, VacuumVolumeCommitRequest, VacuumVolumeCommitResponse,
    VacuumVolumeCompactRequest, VacuumVolumeCompactResponse,
};
use serde::Serialize;
use tokio::sync::RwLock;

use crate::{
    errors::{Error, Result},
    storage::{erasure_coding::EcVolumeInfo, VolumeError, VolumeId, VolumeInfo},
    topology::rack::WeakRackRef,
};

#[derive(Serialize)]
pub struct DataNode {
    pub id: FastStr,
    pub ip: FastStr,
    pub port: u16,
    pub public_url: FastStr,
    pub last_seen: i64,
    pub max_volumes: i64,
    max_volume_id: VolumeId,
    #[serde(skip)]
    pub rack: WeakRackRef,
    volumes: HashMap<VolumeId, VolumeInfo>,
    pub ec_shards: HashMap<VolumeId, EcVolumeInfo>,
    pub ec_shard_count: AtomicU64,
    #[serde(skip)]
    client: VolumeServerClient<LoadBalancedChannel>,
}

impl std::fmt::Display for DataNode {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DataNode({})", self.id)
    }
}

impl DataNode {
    pub async fn new(
        id: FastStr,
        ip: FastStr,
        port: u16,
        public_url: FastStr,
        max_volumes: i64,
    ) -> Result<DataNode> {
        let channel = LoadBalancedChannel::builder((ip.to_string(), port + 1))
            .channel()
            .await
            .map_err(|err| Error::String(err.to_string()))?;
        Ok(DataNode {
            id,
            ip,
            port,
            public_url,
            last_seen: 0,
            max_volumes,
            rack: WeakRackRef::new(),
            max_volume_id: 0,
            volumes: HashMap::new(),
            ec_shards: HashMap::new(),
            ec_shard_count: AtomicU64::new(0),
            client: VolumeServerClient::new(channel),
        })
    }

    pub fn url(&self) -> String {
        format!("{}:{}", self.ip, self.port)
    }

    pub fn free_volumes(&self) -> i64 {
        self.max_volumes - self.has_volumes()
    }

    pub async fn update_volumes(&mut self, volume_infos: Vec<VolumeInfo>) -> Vec<VolumeInfo> {
        let mut volumes = HashSet::new();
        for info in volume_infos.iter() {
            volumes.insert(info.id);
        }

        let mut deleted_id = vec![];
        let mut deleted = vec![];

        for (id, volume) in self.volumes.iter_mut() {
            if !volumes.contains(id) {
                deleted_id.push(volume.id)
            }
        }

        for info in volume_infos {
            self.add_or_update_volume(info).await;
        }

        for id in deleted_id.iter() {
            if let Some(volume) = self.volumes.remove(id) {
                deleted.push(volume);
            }
        }

        deleted
    }

    pub async fn add_or_update_volume(&mut self, v: VolumeInfo) {
        self.adjust_max_volume_id(v.id).await;
        self.volumes.insert(v.id, v);
    }

    pub async fn adjust_max_volume_id(&mut self, vid: VolumeId) {
        if vid > self.max_volume_id {
            self.max_volume_id = vid;
        }

        if let Some(rack) = self.rack.upgrade() {
            rack.write()
                .await
                .adjust_max_volume_id(self.max_volume_id)
                .await;
        }
    }

    pub fn has_volumes(&self) -> i64 {
        self.volumes.len() as i64
    }

    pub fn get_volume(&self, vid: VolumeId) -> Option<VolumeInfo> {
        self.volumes.get(&vid).cloned()
    }

    pub async fn rack_id(&self) -> FastStr {
        match self.rack.upgrade() {
            Some(rack) => rack.read().await.id.clone(),
            None => FastStr::empty(),
        }
    }
    pub async fn data_center_id(&self) -> FastStr {
        match self.rack.upgrade() {
            Some(rack) => rack.read().await.data_center_id().await,
            None => FastStr::empty(),
        }
    }
    pub fn set_rack(&mut self, rack: WeakRackRef) {
        self.rack = rack;
    }

    pub async fn allocate_volume(
        &mut self,
        request: AllocateVolumeRequest,
    ) -> StdResult<AllocateVolumeResponse, VolumeError> {
        let response = self.client.allocate_volume(request).await?;
        Ok(response.into_inner())
    }

    pub async fn vacuum_volume_check(
        &mut self,
        request: VacuumVolumeCheckRequest,
    ) -> StdResult<VacuumVolumeCheckResponse, VolumeError> {
        let response = self.client.vacuum_volume_check(request).await?;
        Ok(response.into_inner())
    }

    pub async fn vacuum_volume_compact(
        &mut self,
        request: VacuumVolumeCompactRequest,
    ) -> StdResult<VacuumVolumeCompactResponse, VolumeError> {
        let response = self.client.vacuum_volume_compact(request).await?;
        Ok(response.into_inner())
    }

    pub async fn vacuum_volume_commit(
        &mut self,
        request: VacuumVolumeCommitRequest,
    ) -> StdResult<VacuumVolumeCommitResponse, VolumeError> {
        let response = self.client.vacuum_volume_commit(request).await?;
        Ok(response.into_inner())
    }

    pub async fn vacuum_volume_cleanup(
        &mut self,
        request: VacuumVolumeCleanupRequest,
    ) -> StdResult<VacuumVolumeCleanupResponse, VolumeError> {
        let response = self.client.vacuum_volume_cleanup(request).await?;
        Ok(response.into_inner())
    }
}

#[derive(Clone)]
pub struct DataNodeRef(Arc<RwLock<DataNode>>);

impl DataNodeRef {
    pub async fn new(
        id: FastStr,
        ip: FastStr,
        port: u16,
        public_url: FastStr,
        max_volumes: i64,
    ) -> Result<Self> {
        Ok(Self(Arc::new(RwLock::new(
            DataNode::new(id, ip, port, public_url, max_volumes).await?,
        ))))
    }

    pub async fn read(&self) -> tokio::sync::RwLockReadGuard<'_, DataNode> {
        self.0.read().await
    }

    pub async fn write(&self) -> tokio::sync::RwLockWriteGuard<'_, DataNode> {
        self.0.write().await
    }
}
