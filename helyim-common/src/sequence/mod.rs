mod memory;
pub use memory::MemorySequencer;

mod snowflake;
pub use snowflake::SnowflakeSequencer;

pub trait Sequence {
    fn next_file_id(&self, count: u64) -> Result<u64, SequenceError>;
    fn set_max(&self, value: u64);
}

#[derive(thiserror::Error, Debug)]
pub enum SequenceError {
    #[error("Snowflake error: {0}")]
    Snowflake(#[from] sonyflake::Error),
}

#[derive(Copy, Clone)]
pub enum SequencerType {
    Memory,
    Snowflake,
}

#[derive(Clone)]
pub enum Sequencer {
    Memory(MemorySequencer),
    Snowflake(SnowflakeSequencer),
}

impl Sequencer {
    pub fn new(typ: SequencerType) -> Result<Self, SequenceError> {
        match typ {
            SequencerType::Snowflake => Ok(Sequencer::Snowflake(SnowflakeSequencer::new()?)),
            SequencerType::Memory => Ok(Sequencer::Memory(MemorySequencer::new())),
        }
    }
}

impl Sequence for Sequencer {
    fn next_file_id(&self, count: u64) -> Result<u64, SequenceError> {
        match self {
            Sequencer::Memory(memory) => memory.next_file_id(count),
            Sequencer::Snowflake(snowflake) => snowflake.next_file_id(count),
        }
    }

    fn set_max(&self, value: u64) {
        match self {
            Sequencer::Memory(memory) => memory.set_max(value),
            Sequencer::Snowflake(snowflake) => snowflake.set_max(value),
        }
    }
}
