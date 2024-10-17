use crate::sequence::{Sequence, SequenceError};

#[derive(Clone)]
pub struct SnowflakeSequencer {
    flake: sonyflake::Sonyflake,
}

impl SnowflakeSequencer {
    pub fn new() -> Result<Self, sonyflake::Error> {
        Ok(Self {
            flake: sonyflake::Sonyflake::new()?,
        })
    }
}

impl Sequence for SnowflakeSequencer {
    fn next_file_id(&self, _count: u64) -> Result<u64, SequenceError> {
        Ok(self.flake.next_id()?)
    }

    fn set_max(&self, _seen_value: u64) {
        // ignore set max as we are snowflake
    }
}
