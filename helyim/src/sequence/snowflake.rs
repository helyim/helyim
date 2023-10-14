use crate::{directory::Sequence, errors::Result};

#[derive(Clone)]
pub struct SnowflakeSequencer {
    flake: sonyflake::Sonyflake,
}

impl SnowflakeSequencer {
    pub fn new() -> Result<Self> {
        Ok(Self {
            flake: sonyflake::Sonyflake::new()?,
        })
    }
}

impl Sequence for SnowflakeSequencer {
    fn next_file_id(&self, _count: u64) -> Result<u64> {
        Ok(self.flake.next_id()?)
    }

    fn set_max(&self, _seen_value: u64) {
        // ignore set max as we are snowflake
    }

    fn peek(&self) -> Result<u64> {
        Ok(self.flake.next_id()?)
    }
}
