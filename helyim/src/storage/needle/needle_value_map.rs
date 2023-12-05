use std::{fs, os::unix::fs::OpenOptionsExt};

use leapfrog::LeapMap;

use crate::storage::{
    needle::NeedleValue, types::Size, walk_index_file, NeedleError, NeedleId, VolumeError,
};

type Visit = Box<dyn FnMut(&NeedleId, &NeedleValue) -> Result<(), NeedleError>>;

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

    pub fn load_from_index(index_filename: &str) -> Result<Self, VolumeError> {
        let nm = Self::new();
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
