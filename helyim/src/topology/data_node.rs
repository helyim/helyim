use std::collections::{HashMap, HashSet};

use faststr::FastStr;
use futures::channel::mpsc::unbounded;
use ginepro::LoadBalancedChannel;
use helyim_macros::event_fn;
use helyim_proto::{
    volume_server_client::VolumeServerClient, AllocateVolumeRequest, AllocateVolumeResponse,
    VacuumVolumeCheckRequest, VacuumVolumeCheckResponse, VacuumVolumeCleanupRequest,
    VacuumVolumeCleanupResponse, VacuumVolumeCommitRequest, VacuumVolumeCommitResponse,
    VacuumVolumeCompactRequest, VacuumVolumeCompactResponse,
};
use serde::Serialize;

use crate::{
    errors::Result,
    rt_spawn,
    storage::{VolumeId, VolumeInfo},
    topology::rack::WeakRackRef,
};

#[derive(Debug, Serialize)]
pub struct DataNode {
    pub id: FastStr,
    pub ip: FastStr,
    pub port: u16,
    pub public_url: FastStr,
    pub last_seen: i64,
    pub max_volumes: i64,
    #[serde(skip)]
    inner: DataNodeInnerEventTx,
}

impl std::fmt::Display for DataNode {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DataNode({})", self.id)
    }
}

#[derive(Serialize)]
struct DataNodeInner {
    max_volume_id: VolumeId,
    #[serde(skip)]
    rack: WeakRackRef,
    volumes: HashMap<VolumeId, VolumeInfo>,
    #[serde(skip)]
    client: VolumeServerClient<LoadBalancedChannel>,
}

#[event_fn]
impl DataNodeInner {
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

        for (id, volume) in self.volumes.iter_mut() {
            if !volumes.contains(id) {
                deleted_id.push(volume.id)
            }
        }

        for info in volume_infos {
            self.add_or_update_volume(info).await?;
        }

        for id in deleted_id.iter() {
            if let Some(volume) = self.volumes.remove(id) {
                deleted.push(volume);
            }
        }

        Ok(deleted)
    }

    pub async fn add_or_update_volume(&mut self, v: VolumeInfo) -> Result<()> {
        self.adjust_max_volume_id(v.id).await?;
        self.volumes.insert(v.id, v);
        Ok(())
    }

    pub async fn adjust_max_volume_id(&mut self, vid: VolumeId) -> Result<()> {
        if vid > self.max_volume_id {
            self.max_volume_id = vid;
        }

        if let Some(rack) = self.rack.upgrade() {
            rack.write()
                .await
                .adjust_max_volume_id(self.max_volume_id)
                .await?;
        }

        Ok(())
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
    ) -> Result<AllocateVolumeResponse> {
        let response = self.client.allocate_volume(request).await?;
        Ok(response.into_inner())
    }

    pub async fn vacuum_volume_check(
        &mut self,
        request: VacuumVolumeCheckRequest,
    ) -> Result<VacuumVolumeCheckResponse> {
        let response = self.client.vacuum_volume_check(request).await?;
        Ok(response.into_inner())
    }

    pub async fn vacuum_volume_compact(
        &mut self,
        request: VacuumVolumeCompactRequest,
    ) -> Result<VacuumVolumeCompactResponse> {
        let response = self.client.vacuum_volume_compact(request).await?;
        Ok(response.into_inner())
    }

    pub async fn vacuum_volume_commit(
        &mut self,
        request: VacuumVolumeCommitRequest,
    ) -> Result<VacuumVolumeCommitResponse> {
        let response = self.client.vacuum_volume_commit(request).await?;
        Ok(response.into_inner())
    }

    pub async fn vacuum_volume_cleanup(
        &mut self,
        request: VacuumVolumeCleanupRequest,
    ) -> Result<VacuumVolumeCleanupResponse> {
        let response = self.client.vacuum_volume_cleanup(request).await?;
        Ok(response.into_inner())
    }
}

impl DataNode {
    pub async fn new(
        id: FastStr,
        ip: FastStr,
        port: u16,
        public_url: FastStr,
        max_volumes: i64,
        shutdown: async_broadcast::Receiver<()>,
    ) -> Result<DataNode> {
        let (tx, rx) = unbounded();

        let channel = LoadBalancedChannel::builder((ip.to_string(), port + 1))
            .channel()
            .await?;
        let inner = DataNodeInner {
            rack: WeakRackRef::new(),
            max_volume_id: 0,
            volumes: HashMap::new(),
            client: VolumeServerClient::new(channel),
        };
        rt_spawn(data_node_inner_loop(inner, rx, shutdown));

        Ok(DataNode {
            id,
            ip,
            port,
            public_url,
            last_seen: 0,
            max_volumes,
            inner: DataNodeInnerEventTx::new(tx),
        })
    }

    pub fn url(&self) -> String {
        format!("{}:{}", self.ip, self.port)
    }

    pub async fn add_or_update_volume(&self, v: VolumeInfo) -> Result<()> {
        self.inner.add_or_update_volume(v).await
    }

    pub async fn has_volumes(&self) -> Result<i64> {
        self.inner.has_volumes().await
    }

    pub async fn free_volumes(&self) -> Result<i64> {
        Ok(self.max_volumes - self.has_volumes().await?)
    }

    pub async fn rack_id(&self) -> Result<FastStr> {
        self.inner.rack_id().await
    }

    pub async fn data_center_id(&self) -> Result<FastStr> {
        self.inner.data_center_id().await
    }

    pub async fn get_volume(&self, vid: VolumeId) -> Result<Option<VolumeInfo>> {
        self.inner.get_volume(vid).await
    }

    pub fn set_rack(&self, rack: WeakRackRef) -> Result<()> {
        self.inner.set_rack(rack)
    }

    pub async fn update_volumes(&self, volume_infos: Vec<VolumeInfo>) -> Result<Vec<VolumeInfo>> {
        self.inner.update_volumes(volume_infos).await
    }

    pub async fn adjust_max_volume_id(&self, vid: VolumeId) -> Result<()> {
        self.inner.adjust_max_volume_id(vid).await
    }

    pub async fn allocate_volume(
        &self,
        request: AllocateVolumeRequest,
    ) -> Result<AllocateVolumeResponse> {
        self.inner.allocate_volume(request).await
    }

    pub async fn vacuum_volume_check(
        &self,
        request: VacuumVolumeCheckRequest,
    ) -> Result<VacuumVolumeCheckResponse> {
        self.inner.vacuum_volume_check(request).await
    }

    pub async fn vacuum_volume_compact(
        &self,
        request: VacuumVolumeCompactRequest,
    ) -> Result<VacuumVolumeCompactResponse> {
        self.inner.vacuum_volume_compact(request).await
    }

    pub async fn vacuum_volume_commit(
        &self,
        request: VacuumVolumeCommitRequest,
    ) -> Result<VacuumVolumeCommitResponse> {
        self.inner.vacuum_volume_commit(request).await
    }

    pub async fn vacuum_volume_cleanup(
        &self,
        request: VacuumVolumeCleanupRequest,
    ) -> Result<VacuumVolumeCleanupResponse> {
        self.inner.vacuum_volume_cleanup(request).await
    }
}
