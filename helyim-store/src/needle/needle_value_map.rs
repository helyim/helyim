use helyim_common::types::{NeedleId, NeedleValue};
use leapfrog::LeapMap;

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
