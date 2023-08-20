mod memory_sequencer;
pub use memory_sequencer::MemorySequencer;

pub trait Sequencer {
    fn next_file_id(&self, count: u64) -> (u64, u64);
    fn set_max(&self, value: u64);
    fn peek(&self) -> u64;
}
