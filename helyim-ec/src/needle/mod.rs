use std::{fs, io, os::unix::fs::OpenOptionsExt};

use helyim_common::types::{NeedleId, NeedleValue, Size, walk_index_file};
use indexmap::IndexMap;
use parking_lot::RwLock;

pub struct SortedIndexMap {
    pub map: RwLock<IndexMap<NeedleId, NeedleValue>>,
}

impl SortedIndexMap {
    pub fn load_from_index(index_filename: &str) -> Result<Self, io::Error> {
        let nm = Self {
            map: RwLock::new(IndexMap::new()),
        };
        let mut index_file = fs::OpenOptions::new()
            .read(true)
            .mode(0o644)
            .open(index_filename)?;
        walk_index_file(
            &mut index_file,
            |needle_id, offset, size: Size| -> Result<(), io::Error> {
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

    pub fn ascending_visit<F>(&self, mut visit: F) -> Result<(), io::Error>
    where
        F: FnMut(&NeedleId, &NeedleValue) -> Result<(), io::Error>,
    {
        self.map.write().sort_by(|k1, _, k2, _| k1.cmp(k2));
        for (key, value) in self.map.read().iter() {
            visit(key, value)?;
        }
        Ok(())
    }
}

impl SortedIndexMap {
    fn set(&self, key: NeedleId, value: NeedleValue) -> Option<NeedleValue> {
        self.map.write().insert(key, value)
    }

    fn delete(&self, key: NeedleId) -> Option<NeedleValue> {
        self.map.write().shift_remove(&key)
    }
}
