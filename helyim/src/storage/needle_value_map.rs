use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;

use crate::{
    errors::Result,
    storage::{needle::NeedleValue, NeedleId},
};

type Visit = Box<dyn FnMut(&NeedleId, &NeedleValue) -> Result<()>>;

pub trait NeedleValueMap: Send + Sync {
    fn set(&mut self, key: NeedleId, value: NeedleValue) -> Option<NeedleValue>;
    fn delete(&mut self, key: NeedleId) -> Option<NeedleValue>;
    fn get(&self, key: NeedleId) -> Option<NeedleValue>;
}

#[derive(Default)]
pub struct MemoryNeedleValueMap {
    map: Arc<RwLock<HashMap<NeedleId, NeedleValue>>>,
}

impl MemoryNeedleValueMap {
    pub fn new() -> Self {
        Self::default()
    }
}

impl NeedleValueMap for MemoryNeedleValueMap {
    fn set(&mut self, key: NeedleId, value: NeedleValue) -> Option<NeedleValue> {
        self.map.write().insert(key, value)
    }

    fn delete(&mut self, key: NeedleId) -> Option<NeedleValue> {
        self.map.write().remove(&key)
    }

    fn get(&self, key: NeedleId) -> Option<NeedleValue> {
        self.map.read().get(&key).copied()
    }
}
