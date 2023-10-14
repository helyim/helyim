use crate::errors::Result;

mod memory;
pub use memory::MemorySequencer;

mod snowflake;
pub use snowflake::SnowflakeSequencer;

pub trait Sequence {
    fn next_file_id(&self, count: u64) -> Result<u64>;
    fn set_max(&self, value: u64);
    fn peek(&self) -> Result<u64>;
}

#[derive(Clone)]
pub enum Sequencer {
    Memory(MemorySequencer),
    Snowflake(SnowflakeSequencer),
}

impl Sequencer {
    pub fn new(typ: &str) -> Result<Self> {
        let typ = typ.to_lowercase();
        match typ.as_str() {
            "memory" => Ok(Sequencer::Memory(MemorySequencer::new())),
            _ => Ok(Sequencer::Snowflake(SnowflakeSequencer::new()?)),
        }
    }
}

impl Sequence for Sequencer {
    fn next_file_id(&self, count: u64) -> Result<u64> {
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

    fn peek(&self) -> Result<u64> {
        match self {
            Sequencer::Memory(memory) => memory.peek(),
            Sequencer::Snowflake(snowflake) => snowflake.peek(),
        }
    }
}
