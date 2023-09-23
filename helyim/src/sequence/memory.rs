use std::sync::Arc;

use parking_lot::Mutex;

use crate::{directory::Sequence, errors::Result};

#[derive(Clone)]
pub struct MemorySequencer {
    counter: Arc<Mutex<u64>>,
}

impl MemorySequencer {
    pub fn new() -> MemorySequencer {
        MemorySequencer {
            counter: Arc::new(Mutex::new(1)),
        }
    }
}

impl Default for MemorySequencer {
    fn default() -> Self {
        Self::new()
    }
}

impl Sequence for MemorySequencer {
    fn next_file_id(&self, count: u64) -> Result<u64> {
        let mut counter = self.counter.lock();
        let file_id = *counter;
        *counter += count;
        Ok(file_id)
    }

    fn set_max(&self, seen_value: u64) {
        let mut counter = self.counter.lock();
        if *counter <= seen_value {
            *counter = seen_value;
        }
    }

    fn peek(&self) -> Result<u64> {
        let counter = self.counter.lock();

        Ok(*counter)
    }
}
