use std::collections::{HashMap, HashSet};

use faststr::FastStr;
use futures::{
    channel::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    StreamExt,
};
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
    directory::topology::RackEventTx,
    errors::Result,
    storage::{VolumeId, VolumeInfo},
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

    pub async fn data_center_id(&self) -> Result<FastStr> {
        match self.rack.as_ref() {
            Some(rack) => rack.data_center_id().await,
            None => Ok(FastStr::empty()),
        }
    }

    pub async fn update_volumes(&mut self, infos: Vec<VolumeInfo>) -> Result<Vec<VolumeInfo>> {
        let mut volumes = HashSet::new();
        for info in infos.iter() {
            volumes.insert(info.id);
        }

        let mut deleted_id = vec![];
        let mut deleted = vec![];

        for (id, volume) in self.volumes.iter_mut() {
            if !volumes.contains(id) {
                deleted_id.push(volume.id)
            }
        }

        for vi in infos {
            self.add_or_update_volume(vi).await?;
        }

        for id in deleted_id.iter() {
            if let Some(volume) = self.volumes.remove(id) {
                deleted.push(volume);
            }
        }

        Ok(deleted)
    }

    pub fn grpc_addr(&self) -> String {
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

pub enum DataNodeEvent {
    HasVolumes(oneshot::Sender<i64>),
    MaxVolumes(oneshot::Sender<i64>),
    FreeVolumes(oneshot::Sender<i64>),
    PublicUrl(oneshot::Sender<FastStr>),
    AddOrUpdateVolume(VolumeInfo, oneshot::Sender<Result<()>>),
    Ip(oneshot::Sender<FastStr>),
    Port(oneshot::Sender<u16>),
    GetVolume(VolumeId, oneshot::Sender<Option<VolumeInfo>>),
    Id(oneshot::Sender<FastStr>),
    RackId(oneshot::Sender<Result<FastStr>>),
    DataCenterId(oneshot::Sender<Result<FastStr>>),
    SetRack(RackEventTx),
    UpdateVolumes(Vec<VolumeInfo>, oneshot::Sender<Result<Vec<VolumeInfo>>>),

    // Volume operation
    AllocateVolume(
        AllocateVolumeRequest,
        oneshot::Sender<Result<AllocateVolumeResponse>>,
    ),
    VacuumVolumeCheck(
        VacuumVolumeCheckRequest,
        oneshot::Sender<Result<VacuumVolumeCheckResponse>>,
    ),
    VacuumVolumeCompact(
        VacuumVolumeCompactRequest,
        oneshot::Sender<Result<VacuumVolumeCompactResponse>>,
    ),
    VacuumVolumeCommit(
        VacuumVolumeCommitRequest,
        oneshot::Sender<Result<VacuumVolumeCommitResponse>>,
    ),
    VacuumVolumeCleanup(
        VacuumVolumeCleanupRequest,
        oneshot::Sender<Result<VacuumVolumeCleanupResponse>>,
    ),
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
                    DataNodeEvent::HasVolumes(tx) => {
                        let _ = tx.send(data_node.has_volumes());
                    }
                    DataNodeEvent::MaxVolumes(tx) => {
                        let _ = tx.send(data_node.max_volumes());
                    }
                    DataNodeEvent::FreeVolumes(tx) => {
                        let _ = tx.send(data_node.free_volumes());
                    }
                    DataNodeEvent::PublicUrl(tx) => {
                        let _ = tx.send(data_node.public_url.clone());
                    }
                    DataNodeEvent::AddOrUpdateVolume(v, tx) => {
                        let _ = tx.send(data_node.add_or_update_volume(v).await);
                    }
                    DataNodeEvent::Ip(tx) => {
                        let _ = tx.send(data_node.ip.clone());
                    }
                    DataNodeEvent::Port(tx) => {
                        let _ = tx.send(data_node.port);
                    }
                    DataNodeEvent::GetVolume(vid, tx) => {
                        let _ = tx.send(data_node.volumes.get(&vid).cloned());
                    }
                    DataNodeEvent::Id(tx) => {
                        let _ = tx.send(data_node.id.clone());
                    }
                    DataNodeEvent::RackId(tx) => {
                        let _ = tx.send(data_node.rack_id().await);
                    }
                    DataNodeEvent::DataCenterId(tx) => {
                        let _ = tx.send(data_node.data_center_id().await);
                    }
                    DataNodeEvent::SetRack(tx) => {
                        data_node.rack = Some(tx);
                    }
                    DataNodeEvent::UpdateVolumes(volumes, tx) => {
                        let _ = tx.send(data_node.update_volumes(volumes).await);
                    }
                    DataNodeEvent::AllocateVolume(request, tx) => {
                        let _ = tx.send(data_node.allocate_volume(request).await);
                    }
                    DataNodeEvent::VacuumVolumeCheck(request, tx) => {
                        let _ = tx.send(data_node.vacuum_volume_check(request).await);
                    }
                    DataNodeEvent::VacuumVolumeCompact(request, tx) => {
                        let _ = tx.send(data_node.vacuum_volume_compact(request).await);
                    }
                    DataNodeEvent::VacuumVolumeCommit(request, tx) => {
                        let _ = tx.send(data_node.vacuum_volume_commit(request).await);
                    }
                    DataNodeEvent::VacuumVolumeCleanup(request, tx) => {
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

#[derive(Debug, Clone)]
pub struct DataNodeEventTx(UnboundedSender<DataNodeEvent>);

impl DataNodeEventTx {
    pub fn new(tx: UnboundedSender<DataNodeEvent>) -> Self {
        DataNodeEventTx(tx)
    }

    pub async fn has_volumes(&self) -> Result<i64> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(DataNodeEvent::HasVolumes(tx))?;
        Ok(rx.await?)
    }

    pub async fn max_volumes(&self) -> Result<i64> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(DataNodeEvent::MaxVolumes(tx))?;
        Ok(rx.await?)
    }

    pub async fn free_volumes(&self) -> Result<i64> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(DataNodeEvent::FreeVolumes(tx))?;
        Ok(rx.await?)
    }

    pub async fn url(&self) -> Result<String> {
        Ok(format!(
            "http://{}:{}",
            self.ip().await?,
            self.port().await?
        ))
    }

    pub async fn public_url(&self) -> Result<FastStr> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(DataNodeEvent::PublicUrl(tx))?;
        Ok(rx.await?)
    }

    pub async fn add_or_update_volume(&self, v: VolumeInfo) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0
            .unbounded_send(DataNodeEvent::AddOrUpdateVolume(v, tx))?;
        rx.await?
    }

    pub async fn ip(&self) -> Result<FastStr> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(DataNodeEvent::Ip(tx))?;
        Ok(rx.await?)
    }

    pub async fn port(&self) -> Result<u16> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(DataNodeEvent::Port(tx))?;
        Ok(rx.await?)
    }

    pub async fn get_volume(&self, vid: VolumeId) -> Result<Option<VolumeInfo>> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(DataNodeEvent::GetVolume(vid, tx))?;
        Ok(rx.await?)
    }

    pub async fn id(&self) -> Result<FastStr> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(DataNodeEvent::Id(tx))?;
        Ok(rx.await?)
    }

    pub async fn rack_id(&self) -> Result<FastStr> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(DataNodeEvent::RackId(tx))?;
        rx.await?
    }

    pub async fn data_center_id(&self) -> Result<FastStr> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(DataNodeEvent::DataCenterId(tx))?;
        rx.await?
    }

    pub async fn set_rack(&self, rack: RackEventTx) -> Result<()> {
        self.0.unbounded_send(DataNodeEvent::SetRack(rack))?;
        Ok(())
    }

    pub async fn update_volumes(&self, volumes: Vec<VolumeInfo>) -> Result<Vec<VolumeInfo>> {
        let (tx, rx) = oneshot::channel();
        self.0
            .unbounded_send(DataNodeEvent::UpdateVolumes(volumes, tx))?;
        rx.await?
    }

    pub async fn allocate_volume(
        &self,
        request: AllocateVolumeRequest,
    ) -> Result<AllocateVolumeResponse> {
        let (tx, rx) = oneshot::channel();
        self.0
            .unbounded_send(DataNodeEvent::AllocateVolume(request, tx))?;
        rx.await?
    }

    pub async fn vacuum_volume_check(
        &self,
        request: VacuumVolumeCheckRequest,
    ) -> Result<VacuumVolumeCheckResponse> {
        let (tx, rx) = oneshot::channel();
        self.0
            .unbounded_send(DataNodeEvent::VacuumVolumeCheck(request, tx))?;
        rx.await?
    }

    pub async fn vacuum_volume_compact(
        &self,
        request: VacuumVolumeCompactRequest,
    ) -> Result<VacuumVolumeCompactResponse> {
        let (tx, rx) = oneshot::channel();
        self.0
            .unbounded_send(DataNodeEvent::VacuumVolumeCompact(request, tx))?;
        rx.await?
    }

    pub async fn vacuum_volume_commit(
        &self,
        request: VacuumVolumeCommitRequest,
    ) -> Result<VacuumVolumeCommitResponse> {
        let (tx, rx) = oneshot::channel();
        self.0
            .unbounded_send(DataNodeEvent::VacuumVolumeCommit(request, tx))?;
        rx.await?
    }

    pub async fn vacuum_volume_cleanup(
        &self,
        request: VacuumVolumeCleanupRequest,
    ) -> Result<VacuumVolumeCleanupResponse> {
        let (tx, rx) = oneshot::channel();
        self.0
            .unbounded_send(DataNodeEvent::VacuumVolumeCleanup(request, tx))?;
        rx.await?
    }
}
