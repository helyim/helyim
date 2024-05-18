use std::sync::{
    atomic::{AtomicI64, AtomicU32, Ordering},
    Arc,
};

use faststr::FastStr;
use serde::{Deserialize, Serialize};

use crate::{errors::Result, storage::VolumeId, topology::DataNodeRef};

pub type NodeId = FastStr;

#[derive(Serialize, Deserialize, Clone)]
pub struct NodeImpl {
    pub id: NodeId,
    volume_count: Arc<AtomicI64>,
    active_volume_count: Arc<AtomicI64>,
    ec_shard_count: Arc<AtomicI64>,
    max_volume_count: Arc<AtomicI64>,
    max_volume_id: Arc<AtomicU32>,
}

impl NodeImpl {
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            volume_count: Arc::new(AtomicI64::new(0)),
            active_volume_count: Arc::new(AtomicI64::new(0)),
            ec_shard_count: Arc::new(AtomicI64::new(0)),
            max_volume_count: Arc::new(AtomicI64::new(0)),
            max_volume_id: Arc::new(AtomicU32::new(0)),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub(in crate::topology) fn _volume_count(&self) -> i64 {
        self.volume_count.load(Ordering::Relaxed)
    }

    pub(in crate::topology) fn _adjust_volume_count(&self, volume_count_delta: i64) {
        self.volume_count
            .fetch_add(volume_count_delta, Ordering::Relaxed);
    }

    pub fn _active_volume_count(&self) -> i64 {
        self.active_volume_count.load(Ordering::Relaxed)
    }

    pub(in crate::topology) fn _adjust_active_volume_count(&self, active_volume_count_delta: i64) {
        self.active_volume_count
            .fetch_add(active_volume_count_delta, Ordering::Relaxed);
    }

    pub fn ec_shard_count(&self) -> i64 {
        self.ec_shard_count.load(Ordering::Relaxed)
    }

    pub(in crate::topology) fn _adjust_ec_shard_count(&self, ec_shard_count_delta: i64) {
        self.ec_shard_count
            .fetch_add(ec_shard_count_delta, Ordering::Relaxed);
    }

    pub(in crate::topology) fn _max_volume_count(&self) -> i64 {
        self.max_volume_count.load(Ordering::Relaxed)
    }

    pub fn set_max_volume_count(&self, max_volume_count: i64) {
        self.max_volume_count
            .store(max_volume_count, Ordering::Relaxed);
    }

    pub(in crate::topology) fn _adjust_max_volume_count(&self, max_volume_count_delta: i64) {
        self.max_volume_count
            .fetch_add(max_volume_count_delta, Ordering::Relaxed);
    }

    pub fn max_volume_id(&self) -> u32 {
        self.max_volume_id.load(Ordering::Relaxed)
    }

    pub(in crate::topology) fn _adjust_max_volume_id(&self, volume_id: u32) {
        if self.max_volume_id() < volume_id {
            self.max_volume_id.store(volume_id, Ordering::Relaxed);
        }
    }
}

pub enum NodeType {
    DataNode,
    Rack,
    DataCenter,
}

#[async_trait::async_trait]
pub trait Node {
    fn id(&self) -> &str;
    fn free_space(&self) -> i64;
    fn reserve_one_volume(&self, rand: i64) -> Result<Option<DataNodeRef>>;
    async fn adjust_max_volume_count(&self, max_volume_count_delta: i64);
    async fn adjust_volume_count(&self, volume_count_delta: i64);
    async fn adjust_ec_shard_count(&self, ec_shard_count_delta: i64);
    async fn adjust_active_volume_count(&self, active_volume_count_delta: i64);
    async fn adjust_max_volume_id(&self, vid: VolumeId);

    fn volume_count(&self) -> i64;
    fn ec_shard_count(&self) -> i64;
    fn active_volume_count(&self) -> i64;
    fn max_volume_count(&self) -> i64;
    fn max_volume_id(&self) -> VolumeId;

    fn set_parent(&mut self, parent: Arc<dyn Node>);
    fn link_child_node(&mut self, child: Arc<dyn Node>);
    fn unlink_child_node(&mut self, node_id: NodeId);

    fn node_type(&self) -> NodeType;
    fn children(&self) -> Vec<Arc<dyn Node>>;
    fn parent(&self) -> Option<Arc<dyn Node>>;
}
