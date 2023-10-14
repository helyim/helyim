use std::{
    collections::{btree_map::Iter, BTreeMap},
    fs,
    os::unix::fs::OpenOptionsExt,
};

use crate::{
    errors::Result,
    storage::{needle::NeedleValue, walk_index_file, NeedleId},
};

type Visit = Box<dyn FnMut(&NeedleId, &NeedleValue) -> Result<()>>;

pub trait NeedleValueMap: Send {
    fn set(&mut self, key: NeedleId, value: NeedleValue) -> Option<NeedleValue>;
    fn delete(&mut self, key: NeedleId) -> Option<NeedleValue>;
    fn get(&self, key: NeedleId) -> Option<NeedleValue>;
}

#[derive(Default)]
pub struct MemoryNeedleValueMap {
    map: BTreeMap<NeedleId, NeedleValue>,
}

impl MemoryNeedleValueMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn load_from_index(index_filename: &str) -> Result<Self> {
        let mut nm = Self::new();
        let index_file = fs::OpenOptions::new()
            .read(true)
            .mode(0o644)
            .open(index_filename)?;
        walk_index_file(&index_file, |needle_id, offset, size| -> Result<()> {
            if offset == 0 || size.is_deleted() {
                nm.delete(needle_id);
            } else {
                nm.set(needle_id, NeedleValue { offset, size });
            }
            Ok(())
        })?;
        Ok(nm)
    }
}

impl NeedleValueMap for MemoryNeedleValueMap {
    fn set(&mut self, key: NeedleId, value: NeedleValue) -> Option<NeedleValue> {
        self.map.insert(key, value)
    }

    fn delete(&mut self, key: NeedleId) -> Option<NeedleValue> {
        self.map.remove(&key)
    }

    fn get(&self, key: NeedleId) -> Option<NeedleValue> {
        self.map.get(&key).copied()
    }
}

impl MemoryNeedleValueMap {
    pub fn iter(&self) -> Iter<'_, NeedleId, NeedleValue> {
        self.map.iter()
    }
}
