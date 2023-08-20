use std::sync::Arc;

use parking_lot::Mutex;

use crate::directory::Sequencer;

#[derive(Debug, Clone)]
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

impl Sequencer for MemorySequencer {
    fn next_file_id(&self, count: u64) -> (u64, u64) {
        let mut counter = self.counter.lock();
        let ret = *counter;
        *counter += count;
        (ret, count)
    }

    fn set_max(&self, seen_value: u64) {
        let mut counter = self.counter.lock();
        if *counter <= seen_value {
            *counter = seen_value;
        }
    }

    fn peek(&self) -> u64 {
        let counter = self.counter.lock();

        *counter
    }
}
