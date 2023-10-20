use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicU32, Ordering},
        Weak,
    },
};

use async_lock::RwLock;
use dashmap::DashMap;
use faststr::FastStr;
use helyim_proto::{
    volume_server_client::VolumeServerClient, AllocateVolumeRequest, AllocateVolumeResponse,
    VacuumVolumeCheckRequest, VacuumVolumeCheckResponse, VacuumVolumeCleanupRequest,
    VacuumVolumeCleanupResponse, VacuumVolumeCommitRequest, VacuumVolumeCommitResponse,
    VacuumVolumeCompactRequest, VacuumVolumeCompactResponse,
};
use serde::Serialize;
use tonic::transport::Channel;

use crate::{
    errors::Result,
    storage::{VolumeId, VolumeInfo},
    topology::Rack,
};

#[derive(Debug, Serialize)]
pub struct DataNode {
    pub id: FastStr,
    pub ip: FastStr,
    pub port: u16,
    pub public_url: FastStr,
    last_seen: i64,
    #[serde(skip)]
    pub rack: Weak<RwLock<Rack>>,
    #[serde(skip)]
    volumes: DashMap<VolumeId, VolumeInfo>,
    max_volumes: i64,
    max_volume_id: AtomicU32,
    #[serde(skip)]
    client: Option<VolumeServerClient<Channel>>,
}

unsafe impl Send for DataNode {}

impl std::fmt::Display for DataNode {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "({}, volumes: {})", self.id, self.volumes.len())
    }
}

impl DataNode {
    pub fn new(
        id: FastStr,
        ip: FastStr,
        port: u16,
        public_url: FastStr,
        max_volumes: i64,
    ) -> DataNode {
        DataNode {
            id,
            ip,
            port,
            public_url,
            last_seen: 0,
            rack: Weak::new(),
            volumes: DashMap::new(),
            max_volumes,
            max_volume_id: AtomicU32::new(0),
            client: None,
        }
    }

    pub fn url(&self) -> String {
        format!("http://{}:{}", self.ip, self.port)
    }

    pub async fn add_or_update_volume(&mut self, v: VolumeInfo) -> Result<()> {
        self.adjust_max_volume_id(v.id).await?;
        self.volumes.insert(v.id, v);
        Ok(())
    }

    pub fn has_volumes(&self) -> i64 {
        self.volumes.len() as i64
    }

    pub fn max_volumes(&self) -> i64 {
        self.max_volumes
    }

    pub fn free_volumes(&self) -> i64 {
        self.max_volumes() - self.has_volumes()
    }

    pub async fn rack_id(&self) -> FastStr {
        match self.rack.upgrade() {
            Some(rack) => rack.read().await.id.clone(),
            None => FastStr::empty(),
        }
    }

    pub async fn data_center_id(&self) -> FastStr {
        match self.rack.upgrade() {
            Some(rack) => rack.read().await.data_center_id(),
            None => FastStr::empty(),
        }
    }

    pub fn get_volume(&self, vid: VolumeId) -> Option<VolumeInfo> {
        self.volumes.get(&vid).map(|v| v.value().clone())
    }

    pub async fn update_volumes(
        &mut self,
        volume_infos: Vec<VolumeInfo>,
    ) -> Result<Vec<VolumeInfo>> {
        let mut volumes = HashSet::new();
        for info in volume_infos.iter() {
            volumes.insert(info.id);
        }

        let mut deleted_id = vec![];
        let mut deleted = vec![];

        for entry in self.volumes.iter() {
            let id = entry.key();
            if !volumes.contains(id) {
                deleted_id.push(*id);
            }
        }

        for vi in volume_infos {
            self.add_or_update_volume(vi).await?;
        }

        for id in deleted_id.iter() {
            if let Some((_, volume)) = self.volumes.remove(id) {
                deleted.push(volume);
            }
        }

        Ok(deleted)
    }

    async fn adjust_max_volume_id(&self, vid: VolumeId) -> Result<()> {
        if vid > self.max_volume_id.load(Ordering::Relaxed) {
            self.max_volume_id.store(vid, Ordering::Relaxed);
        }

        if let Some(rack) = self.rack.upgrade() {
            rack.read()
                .await
                .adjust_max_volume_id(self.max_volume_id.load(Ordering::Relaxed))?;
        }

        Ok(())
    }

    fn grpc_addr(&self) -> String {
        format!("http://{}:{}", self.ip, self.port + 1)
    }

    pub async fn allocate_volume(
        &mut self,
        request: AllocateVolumeRequest,
    ) -> Result<AllocateVolumeResponse> {
        let client = self
            .client
            .get_or_insert(VolumeServerClient::connect(self.grpc_addr()).await?);
        let response = client.allocate_volume(request).await?;
        Ok(response.into_inner())
    }

    pub async fn vacuum_volume_check(
        &mut self,
        request: VacuumVolumeCheckRequest,
    ) -> Result<VacuumVolumeCheckResponse> {
        let client = self
            .client
            .get_or_insert(VolumeServerClient::connect(self.grpc_addr()).await?);
        let response = client.vacuum_volume_check(request).await?;
        Ok(response.into_inner())
    }

    pub async fn vacuum_volume_compact(
        &mut self,
        request: VacuumVolumeCompactRequest,
    ) -> Result<VacuumVolumeCompactResponse> {
        let client = self
            .client
            .get_or_insert(VolumeServerClient::connect(self.grpc_addr()).await?);
        let response = client.vacuum_volume_compact(request).await?;
        Ok(response.into_inner())
    }

    pub async fn vacuum_volume_commit(
        &mut self,
        request: VacuumVolumeCommitRequest,
    ) -> Result<VacuumVolumeCommitResponse> {
        let client = self
            .client
            .get_or_insert(VolumeServerClient::connect(self.grpc_addr()).await?);
        let response = client.vacuum_volume_commit(request).await?;
        Ok(response.into_inner())
    }

    pub async fn vacuum_volume_cleanup(
        &mut self,
        request: VacuumVolumeCleanupRequest,
    ) -> Result<VacuumVolumeCleanupResponse> {
        let client = self
            .client
            .get_or_insert(VolumeServerClient::connect(self.grpc_addr()).await?);
        let response = client.vacuum_volume_cleanup(request).await?;
        Ok(response.into_inner())
    }
}
