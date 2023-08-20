use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::errors::{Error, Result};

#[derive(Serialize, Deserialize, Debug, Default, Copy, Clone)]
pub struct ReplicaPlacement {
    pub same_rack_count: u8,
    pub diff_rack_count: u8,
    pub diff_data_center_count: u8,
}

impl ReplicaPlacement {
    pub fn from_u8(u: u8) -> Result<ReplicaPlacement> {
        let s = format!("{:03}", u);
        ReplicaPlacement::new(&s)
    }

    pub fn new(s: &str) -> Result<ReplicaPlacement> {
        if s.len() != 3 {
            return Err(Error::ParseReplicaPlacement(String::from(s)));
        }

        let bytes = s.as_bytes();

        let rp = ReplicaPlacement {
            diff_data_center_count: bytes[0] - b'0',
            diff_rack_count: bytes[1] - b'0',
            same_rack_count: bytes[2] - b'0',
        };

        Ok(rp)
    }

    pub fn get_copy_count(&self) -> usize {
        (self.diff_data_center_count + self.diff_rack_count + self.same_rack_count + 1) as usize
    }
}

impl From<ReplicaPlacement> for u8 {
    fn from(value: ReplicaPlacement) -> Self {
        value.diff_data_center_count * 100 + value.diff_rack_count * 10 + value.same_rack_count
    }
}

impl Display for ReplicaPlacement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}{}{}",
            self.diff_data_center_count, self.diff_rack_count, self.same_rack_count
        )
    }
}
