use std::collections::HashMap;

use crate::{errors::Result, storage::needle::NeedleValue};

pub trait NeedleValueMap: Send {
    fn set(&mut self, key: u64, value: NeedleValue) -> Option<NeedleValue>;
    fn delete(&mut self, key: u64) -> Option<NeedleValue>;
    fn get(&self, key: u64) -> Option<NeedleValue>;

    fn ascending_visit<F>(&self, visit: F) -> Result<()>
    where
        F: Fn(NeedleValue) -> Result<()>;
}

#[derive(Default)]
pub struct MemoryNeedleValueMap {
    hm: HashMap<u64, NeedleValue>,
}

impl MemoryNeedleValueMap {
    pub fn new() -> Self {
        Self::default()
    }
}

impl NeedleValueMap for MemoryNeedleValueMap {
    fn set(&mut self, key: u64, value: NeedleValue) -> Option<NeedleValue> {
        self.hm.insert(key, value)
    }

    fn delete(&mut self, key: u64) -> Option<NeedleValue> {
        self.hm.remove(&key)
    }

    fn get(&self, key: u64) -> Option<NeedleValue> {
        self.hm.get(&key).copied()
    }

    fn ascending_visit<F>(&self, visit: F) -> Result<()> where F: Fn(NeedleValue) -> Result<()> {
        todo!()
    }
}
