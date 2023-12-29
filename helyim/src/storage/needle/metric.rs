use std::sync::atomic::{AtomicU64, Ordering};

use crate::storage::{types::Size, NeedleId};

#[derive(Default)]
pub struct Metric {
    max_file_key: AtomicU64,
    file_count: AtomicU64,
    deleted_count: AtomicU64,
    deleted_bytes: AtomicU64,
    file_bytes: AtomicU64,
}

impl Metric {
    pub fn file_count(&self) -> u64 {
        self.file_count.load(Ordering::Relaxed)
    }

    pub fn deleted_count(&self) -> u64 {
        self.deleted_count.load(Ordering::Relaxed)
    }

    pub fn deleted_bytes(&self) -> u64 {
        self.deleted_bytes.load(Ordering::Relaxed)
    }

    pub fn max_file_key(&self) -> NeedleId {
        self.max_file_key.load(Ordering::Relaxed)
    }

    pub fn maybe_max_file_key(&self, key: u64) {
        if key > self.max_file_key() {
            self.max_file_key.store(key, Ordering::Relaxed);
        }
    }

    pub fn file_bytes(&self) -> u64 {
        self.file_bytes.load(Ordering::Relaxed)
    }

    pub fn add_file(&self, size: Size) {
        self.file_count.fetch_add(1, Ordering::Relaxed);
        self.file_bytes
            .fetch_add(size.actual_size(), Ordering::Relaxed);
    }

    pub fn delete_file(&self, size: Size) {
        self.deleted_count.fetch_add(1, Ordering::Relaxed);
        self.deleted_bytes
            .fetch_add(size.actual_size(), Ordering::Relaxed);
    }
}
