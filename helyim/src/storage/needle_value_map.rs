use std::collections::BTreeMap;

use crate::{errors::Result, storage::needle::NeedleValue};

type Visit = Box<dyn Fn(&NeedleValue) -> Result<()>>;

pub trait NeedleValueMap: Send {
    fn set(&mut self, key: u64, value: NeedleValue) -> Option<NeedleValue>;
    fn delete(&mut self, key: u64) -> Option<NeedleValue>;
    fn get(&self, key: u64) -> Option<NeedleValue>;

    fn ascending_visit(&self, visit: Visit) -> Result<()>;
}

#[derive(Default)]
pub struct MemoryNeedleValueMap {
    map: BTreeMap<u64, NeedleValue>,
}

impl MemoryNeedleValueMap {
    pub fn new() -> Self {
        Self::default()
    }
}

impl NeedleValueMap for MemoryNeedleValueMap {
    fn set(&mut self, key: u64, value: NeedleValue) -> Option<NeedleValue> {
        self.map.insert(key, value)
    }

    fn delete(&mut self, key: u64) -> Option<NeedleValue> {
        self.map.remove(&key)
    }

    fn get(&self, key: u64) -> Option<NeedleValue> {
        self.map.get(&key).copied()
    }

    fn ascending_visit(&self, visit: Visit) -> Result<()> {
        for (key, value) in self.map.iter() {
            visit(value)?;
        }
        Ok(())
    }
}
