use std::{
    collections::{HashMap, HashSet},
    ops::{Deref, DerefMut},
    result::Result as StdResult,
    sync::{atomic::AtomicU64, Arc, Weak},
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
    topology::{node::Node, rack::SimpleRack},
};

#[derive(Serialize)]
pub struct DataNode {
    pub ip: FastStr,
    pub port: u16,
    pub public_url: FastStr,
    pub last_seen: i64,
    inner: Arc<SimpleDataNode>,
    volumes: HashMap<VolumeId, VolumeInfo>,
    pub ec_shards: HashMap<VolumeId, EcVolumeInfo>,
    pub ec_shard_count: AtomicU64,
    #[serde(skip)]
    client: VolumeServerClient<LoadBalancedChannel>,
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
        let channel = LoadBalancedChannel::builder((ip.to_string(), port + 1))
            .channel()
            .await
            .map_err(|err| Error::String(err.to_string()))?;

        Ok(DataNode {
            ip,
            port,
            public_url,
            last_seen: 0,
            inner: Arc::new(SimpleDataNode::new(id, max_volume_count)),
            volumes: HashMap::new(),
            ec_shards: HashMap::new(),
            ec_shard_count: AtomicU64::new(0),
            client: VolumeServerClient::new(channel),
        })
    }

    pub fn url(&self) -> String {
        format!("{}:{}", self.ip, self.port)
    }

    pub async fn update_volumes(&mut self, volume_infos: Vec<VolumeInfo>) -> Vec<VolumeInfo> {
        let mut volumes = HashSet::new();
        for info in volume_infos.iter() {
            volumes.insert(info.id);
        }

        let mut deleted_id = vec![];
        let mut deleted = vec![];

        for (id, volume) in self.volumes.iter() {
            if !volumes.contains(id) {
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
            if let Some(volume) = self.volumes.remove(id) {
                deleted.push(volume);
            }
        }

        deleted
    }

    #[allow(clippy::map_entry)]
    pub async fn add_or_update_volume(&mut self, v: VolumeInfo) {
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

    pub fn get_volume(&self, vid: VolumeId) -> Option<&VolumeInfo> {
        self.volumes.get(&vid)
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

#[derive(Serialize)]
pub struct SimpleDataNode {
    node: Node,
    rack: Weak<SimpleRack>,
}

impl SimpleDataNode {
    pub fn new(id: FastStr, max_volume_count: u64) -> Self {
        let node = Node::new(id);
        node.set_max_volume_count(max_volume_count);
        Self {
            node,
            rack: Weak::new(),
        }
    }

    pub async fn rack_id(&self) -> FastStr {
        match self.rack.upgrade() {
            Some(rack) => rack.id.clone(),
            None => FastStr::empty(),
        }
    }

    pub async fn data_center_id(&self) -> FastStr {
        match self.rack.upgrade() {
            Some(rack) => rack.data_center_id().await,
            None => FastStr::empty(),
        }
    }

    pub fn set_rack(&self, rack: Weak<SimpleRack>) {
        unsafe {
            let this = self as *const Self as *mut Self;
            (*this).rack = rack;
        }
    }

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

        if let Some(rack) = self.rack.upgrade() {
            rack.adjust_volume_count(volume_count_delta).await;
        }
    }

    pub async fn adjust_active_volume_count(&self, active_volume_count_delta: i64) {
        self._adjust_active_volume_count(active_volume_count_delta);

        if let Some(rack) = self.rack.upgrade() {
            rack.adjust_active_volume_count(active_volume_count_delta)
                .await;
        }
    }

    pub async fn adjust_ec_shard_count(&self, ec_shard_count_delta: i64) {
        self._adjust_ec_shard_count(ec_shard_count_delta);

        if let Some(rack) = self.rack.upgrade() {
            rack.adjust_ec_shard_count(ec_shard_count_delta).await;
        }
    }

    pub async fn adjust_max_volume_count(&self, max_volume_count_delta: i64) {
        self._adjust_max_volume_count(max_volume_count_delta);

        if let Some(rack) = self.rack.upgrade() {
            rack.adjust_max_volume_count(max_volume_count_delta).await;
        }
    }

    pub async fn adjust_max_volume_id(&self, vid: VolumeId) {
        self._adjust_max_volume_id(vid);

        if let Some(rack) = self.rack.upgrade() {
            rack.adjust_max_volume_id(self.max_volume_id()).await;
        }
    }
}

impl Deref for DataNode {
    type Target = Arc<SimpleDataNode>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for DataNode {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Deref for SimpleDataNode {
    type Target = Node;

    fn deref(&self) -> &Self::Target {
        &self.node
    }
}

impl DerefMut for SimpleDataNode {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.node
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
        max_volume_count: u64,
    ) -> Result<Self> {
        Ok(Self(Arc::new(RwLock::new(
            DataNode::new(id, ip, port, public_url, max_volume_count).await?,
        ))))
    }

    pub async fn read(&self) -> tokio::sync::RwLockReadGuard<'_, DataNode> {
        self.0.read().await
    }

    pub async fn write(&self) -> tokio::sync::RwLockWriteGuard<'_, DataNode> {
        self.0.write().await
    }
}
