use std::{
    cmp::Ordering as CmpOrdering,
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc,
    },
};

use faststr::FastStr;
use serde::{Deserialize, Serialize};

pub type NodeId = FastStr;

#[derive(Serialize, Deserialize, Clone)]
pub struct Node {
    pub id: NodeId,
    volume_count: Arc<AtomicU64>,
    active_volume_count: Arc<AtomicU64>,
    ec_shard_count: Arc<AtomicU64>,
    max_volume_count: Arc<AtomicU64>,
    max_volume_id: Arc<AtomicU32>,
}

impl Node {
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            volume_count: Arc::new(AtomicU64::new(0)),
            active_volume_count: Arc::new(AtomicU64::new(0)),
            ec_shard_count: Arc::new(AtomicU64::new(0)),
            max_volume_count: Arc::new(AtomicU64::new(0)),
            max_volume_id: Arc::new(AtomicU32::new(0)),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub(in crate::topology) fn _volume_count(&self) -> u64 {
        self.volume_count.load(Ordering::Relaxed)
    }

    pub(in crate::topology) fn _adjust_volume_count(&self, volume_count_delta: i64) {
        match volume_count_delta.cmp(&0) {
            CmpOrdering::Less => self
                .volume_count
                .fetch_sub(volume_count_delta.wrapping_neg() as u64, Ordering::Relaxed),
            CmpOrdering::Greater => self
                .volume_count
                .fetch_add(volume_count_delta as u64, Ordering::Relaxed),
            CmpOrdering::Equal => 0,
        };
    }

    pub fn active_volume_count(&self) -> u64 {
        self.active_volume_count.load(Ordering::Relaxed)
    }

    pub(in crate::topology) fn _adjust_active_volume_count(&self, active_volume_count_delta: i64) {
        match active_volume_count_delta.cmp(&0) {
            CmpOrdering::Less => self.active_volume_count.fetch_sub(
                active_volume_count_delta.wrapping_neg() as u64,
                Ordering::Relaxed,
            ),
            CmpOrdering::Greater => self
                .active_volume_count
                .fetch_add(active_volume_count_delta as u64, Ordering::Relaxed),
            CmpOrdering::Equal => 0,
        };
    }

    pub fn ec_shard_count(&self) -> u64 {
        self.ec_shard_count.load(Ordering::Relaxed)
    }

    pub(in crate::topology) fn _adjust_ec_shard_count(&self, ec_shard_count_delta: i64) {
        match ec_shard_count_delta.cmp(&0) {
            CmpOrdering::Less => self.ec_shard_count.fetch_sub(
                ec_shard_count_delta.wrapping_neg() as u64,
                Ordering::Relaxed,
            ),
            CmpOrdering::Greater => self
                .ec_shard_count
                .fetch_add(ec_shard_count_delta as u64, Ordering::Relaxed),
            CmpOrdering::Equal => 0,
        };
    }

    pub(in crate::topology) fn _max_volume_count(&self) -> u64 {
        self.max_volume_count.load(Ordering::Relaxed)
    }

    pub fn set_max_volume_count(&self, max_volume_count: u64) {
        self.max_volume_count
            .store(max_volume_count, Ordering::Relaxed);
    }

    pub(in crate::topology) fn _adjust_max_volume_count(&self, max_volume_count_delta: i64) {
        match max_volume_count_delta.cmp(&0) {
            CmpOrdering::Less => self.max_volume_count.fetch_sub(
                max_volume_count_delta.wrapping_neg() as u64,
                Ordering::Relaxed,
            ),
            CmpOrdering::Greater => self
                .max_volume_count
                .fetch_add(max_volume_count_delta as u64, Ordering::Relaxed),
            CmpOrdering::Equal => 0,
        };
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
