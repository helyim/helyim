use std::collections::{HashMap, HashSet};

use faststr::FastStr;
use futures::{channel::mpsc::UnboundedReceiver, StreamExt};
use helyim_macros::event_fn;
use helyim_proto::{
    volume_server_client::VolumeServerClient, AllocateVolumeRequest, AllocateVolumeResponse,
    VacuumVolumeCheckRequest, VacuumVolumeCheckResponse, VacuumVolumeCleanupRequest,
    VacuumVolumeCleanupResponse, VacuumVolumeCommitRequest, VacuumVolumeCommitResponse,
    VacuumVolumeCompactRequest, VacuumVolumeCompactResponse,
};
use serde::Serialize;
use tonic::transport::Channel;
use tracing::info;

use crate::{
    errors::Result,
    storage::{VolumeId, VolumeInfo},
    topology::RackEventTx,
};

#[derive(Debug, Serialize)]
pub struct DataNode {
    id: FastStr,
    ip: FastStr,
    port: u16,
    public_url: FastStr,
    last_seen: i64,
    #[serde(skip)]
    rack: Option<RackEventTx>,
    volumes: HashMap<VolumeId, VolumeInfo>,
    max_volumes: i64,
    max_volume_id: VolumeId,
    #[serde(skip)]
    client: Option<VolumeServerClient<Channel>>,
    #[serde(skip)]
    shutdown: async_broadcast::Receiver<()>,
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
        shutdown: async_broadcast::Receiver<()>,
    ) -> DataNode {
        DataNode {
            id,
            ip,
            port,
            public_url,
            last_seen: 0,
            rack: None,
            volumes: HashMap::new(),
            max_volumes,
            max_volume_id: 0,
            client: None,
            shutdown,
        }
    }

    pub async fn adjust_max_volume_id(&mut self, vid: VolumeId) -> Result<()> {
        if vid > self.max_volume_id {
            self.max_volume_id = vid;
        }

        if let Some(rack) = self.rack.as_ref() {
            rack.adjust_max_volume_id(self.max_volume_id).await?;
        }

        Ok(())
    }

    pub fn grpc_addr(&self) -> String {
        format!("http://{}:{}", self.ip, self.port + 1)
    }
}

#[event_fn]
impl DataNode {
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

    pub async fn rack_id(&self) -> Result<FastStr> {
        match self.rack.as_ref() {
            Some(rack) => rack.id().await,
            None => Ok(FastStr::empty()),
        }
    }

    pub fn public_url(&self) -> FastStr {
        self.public_url.clone()
    }

    pub fn id(&self) -> FastStr {
        self.id.clone()
    }

    pub fn ip(&self) -> FastStr {
        self.ip.clone()
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub async fn data_center_id(&self) -> Result<FastStr> {
        match self.rack.as_ref() {
            Some(rack) => rack.data_center_id().await,
            None => Ok(FastStr::empty()),
        }
    }

    pub fn get_volume(&self, vid: VolumeId) -> Option<VolumeInfo> {
        self.volumes.get(&vid).cloned()
    }

    pub fn set_rack(&mut self, rack: RackEventTx) {
        self.rack = Some(rack);
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

        for (id, volume) in self.volumes.iter_mut() {
            if !volumes.contains(id) {
                deleted_id.push(volume.id)
            }
        }

        for vi in volume_infos {
            self.add_or_update_volume(vi).await?;
        }

        for id in deleted_id.iter() {
            if let Some(volume) = self.volumes.remove(id) {
                deleted.push(volume);
            }
        }

        Ok(deleted)
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

pub async fn data_node_loop(
    mut data_node: DataNode,
    mut data_node_rx: UnboundedReceiver<DataNodeEvent>,
) {
    info!("data node [{}] event loop starting.", data_node.id);
    loop {
        tokio::select! {
            Some(event) = data_node_rx.next() => {
                match event {
                    DataNodeEvent::HasVolumes{tx} => {
                        let _ = tx.send(data_node.has_volumes());
                    }
                    DataNodeEvent::MaxVolumes{tx} => {
                        let _ = tx.send(data_node.max_volumes());
                    }
                    DataNodeEvent::FreeVolumes{tx} => {
                        let _ = tx.send(data_node.free_volumes());
                    }
                    DataNodeEvent::PublicUrl{tx} => {
                        let _ = tx.send(data_node.public_url());
                    }
                    DataNodeEvent::AddOrUpdateVolume{v, tx} => {
                        let _ = tx.send(data_node.add_or_update_volume(v).await);
                    }
                    DataNodeEvent::Ip{tx} => {
                        let _ = tx.send(data_node.ip());
                    }
                    DataNodeEvent::Port{tx} => {
                        let _ = tx.send(data_node.port());
                    }
                    DataNodeEvent::GetVolume{vid, tx} => {
                        let _ = tx.send(data_node.get_volume(vid));
                    }
                    DataNodeEvent::Id{tx} => {
                        let _ = tx.send(data_node.id());
                    }
                    DataNodeEvent::RackId{tx} => {
                        let _ = tx.send(data_node.rack_id().await);
                    }
                    DataNodeEvent::DataCenterId{tx} => {
                        let _ = tx.send(data_node.data_center_id().await);
                    }
                    DataNodeEvent::SetRack{rack} => {
                        data_node.set_rack(rack);
                    }
                    DataNodeEvent::UpdateVolumes{volume_infos, tx} => {
                        let _ = tx.send(data_node.update_volumes(volume_infos).await);
                    }
                    DataNodeEvent::AllocateVolume{request, tx} => {
                        let _ = tx.send(data_node.allocate_volume(request).await);
                    }
                    DataNodeEvent::VacuumVolumeCheck{request, tx} => {
                        let _ = tx.send(data_node.vacuum_volume_check(request).await);
                    }
                    DataNodeEvent::VacuumVolumeCompact{request, tx} => {
                        let _ = tx.send(data_node.vacuum_volume_compact(request).await);
                    }
                    DataNodeEvent::VacuumVolumeCommit{request, tx} => {
                        let _ = tx.send(data_node.vacuum_volume_commit(request).await);
                    }
                    DataNodeEvent::VacuumVolumeCleanup{request, tx} => {
                        let _ = tx.send(data_node.vacuum_volume_cleanup(request).await);
                    }
                }
            }
            _ = data_node.shutdown.recv() => {
                break;
            }
        }
    }
    info!("data node [{}] event loop stopped.", data_node.id);
}
impl DataNodeEventTx {
    pub async fn url(&self) -> Result<String> {
        Ok(format!(
            "http://{}:{}",
            self.ip().await?,
            self.port().await?
        ))
    }
}
