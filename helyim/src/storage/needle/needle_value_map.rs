use std::{fs, os::unix::fs::OpenOptionsExt};

use indexmap::IndexMap;
use leapfrog::LeapMap;
use parking_lot::RwLock;

use crate::storage::{
    needle::NeedleValue, types::Size, walk_index_file, NeedleError, NeedleId, VolumeError,
};

pub trait NeedleValueMap: Send + Sync {
    fn set(&self, key: NeedleId, value: NeedleValue) -> Option<NeedleValue>;
    fn delete(&self, key: NeedleId) -> Option<NeedleValue>;
    fn get(&self, key: NeedleId) -> Option<NeedleValue>;
}

pub struct MemoryNeedleValueMap {
    pub map: LeapMap<NeedleId, NeedleValue>,
}

impl Default for MemoryNeedleValueMap {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryNeedleValueMap {
    pub fn new() -> Self {
        Self {
            map: LeapMap::new(),
        }
    }
}

impl NeedleValueMap for MemoryNeedleValueMap {
    fn set(&self, key: NeedleId, value: NeedleValue) -> Option<NeedleValue> {
        self.map.insert(key, value)
    }

    fn delete(&self, key: NeedleId) -> Option<NeedleValue> {
        self.map.remove(&key)
    }

    fn get(&self, key: NeedleId) -> Option<NeedleValue> {
        match self.map.get(&key) {
            Some(mut value) => value.value(),
            None => None,
        }
    }
}

pub struct SortedIndexMap {
    pub map: RwLock<IndexMap<NeedleId, NeedleValue>>,
}

impl SortedIndexMap {
    pub fn load_from_index(index_filename: &str) -> Result<Self, VolumeError> {
        let nm = Self {
            map: RwLock::new(IndexMap::new()),
        };
        let mut index_file = fs::OpenOptions::new()
            .read(true)
            .mode(0o644)
            .open(index_filename)?;
        walk_index_file(
            &mut index_file,
            |needle_id, offset, size: Size| -> Result<(), NeedleError> {
                if offset == 0 || size.is_deleted() {
                    nm.delete(needle_id);
                } else {
                    nm.set(needle_id, NeedleValue { offset, size });
                }
                Ok(())
            },
        )?;
        Ok(nm)
    }

    pub fn ascending_visit<F>(&self, mut visit: F) -> Result<(), NeedleError>
    where
        F: FnMut(&NeedleId, &NeedleValue) -> Result<(), NeedleError>,
    {
        self.map.write().sort_by(|k1, _, k2, _| k1.cmp(k2));
        for (key, value) in self.map.read().iter() {
            visit(key, value)?;
        }
        Ok(())
    }
}

impl NeedleValueMap for SortedIndexMap {
    fn set(&self, key: NeedleId, value: NeedleValue) -> Option<NeedleValue> {
        self.map.write().insert(key, value)
    }

    fn delete(&self, key: NeedleId) -> Option<NeedleValue> {
        self.map.write().shift_remove(&key)
    }

    fn get(&self, key: NeedleId) -> Option<NeedleValue> {
        self.map.read().get(&key).copied()
    }
}
