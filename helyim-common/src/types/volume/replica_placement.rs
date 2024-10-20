use std::{
    fmt::{Display, Formatter},
    io,
    io::ErrorKind,
};

use serde::{Deserialize, Serialize};

/// The meaning of replication type
///
/// ```txt
/// 000 no replication, just one copy
/// 001 replicate once on the same rack
/// 010 replicate once on a different rack in the same data center
/// 100 replicate once on a different data center
/// 200 replicate twice on two other different data center
/// 110 replicate once on a different rack, and once on a different data center
/// ```
///
/// So if the replication type is xyz
///
/// ```txt
/// x number of replica in other data centers
/// y number of replica in other racks in the same data center
/// z number of replica in other servers in the same rack
/// ```
///
/// x,y,z each can be 0, 1, or 2. So there are 9 possible replication types,
/// and can be easily extended. Each replication type will physically create x+y+z+1 copies
/// of volume data files.
#[derive(Serialize, Deserialize, Debug, Default, Copy, Clone)]
pub struct ReplicaPlacement {
    pub same_rack_count: u8,
    pub diff_rack_count: u8,
    pub diff_data_center_count: u8,
}

impl ReplicaPlacement {
    pub fn from_u8(u: u8) -> Result<ReplicaPlacement, io::Error> {
        let s = format!("{:03}", u);
        ReplicaPlacement::new(&s)
    }

    pub fn new(s: &str) -> Result<ReplicaPlacement, io::Error> {
        if s.len() != 3 {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("Invalid replica placement: {s}"),
            ));
        }

        let bytes = s.as_bytes();

        let rp = ReplicaPlacement {
            diff_data_center_count: bytes[0] - b'0',
            diff_rack_count: bytes[1] - b'0',
            same_rack_count: bytes[2] - b'0',
        };

        Ok(rp)
    }

    pub fn copy_count(&self) -> usize {
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

#[cfg(test)]
mod tests {
    use crate::types::volume::ReplicaPlacement;

    #[test]
    pub fn test_replica_placement() {
        let rp = ReplicaPlacement::new("000").unwrap();
        assert_eq!(rp.copy_count(), 1);
        assert_eq!(rp.diff_data_center_count, 0);
        assert_eq!(rp.diff_rack_count, 0);
        assert_eq!(rp.same_rack_count, 0);

        let rp = ReplicaPlacement::new("001").unwrap();
        assert_eq!(rp.copy_count(), 2);
        assert_eq!(rp.diff_data_center_count, 0);
        assert_eq!(rp.diff_rack_count, 0);
        assert_eq!(rp.same_rack_count, 1);

        let rp = ReplicaPlacement::new("010").unwrap();
        assert_eq!(rp.copy_count(), 2);
        assert_eq!(rp.diff_data_center_count, 0);
        assert_eq!(rp.diff_rack_count, 1);
        assert_eq!(rp.same_rack_count, 0);

        let rp = ReplicaPlacement::new("100").unwrap();
        assert_eq!(rp.copy_count(), 2);
        assert_eq!(rp.diff_data_center_count, 1);
        assert_eq!(rp.diff_rack_count, 0);
        assert_eq!(rp.same_rack_count, 0);

        let rp = ReplicaPlacement::from_u8(211).unwrap();
        assert_eq!(rp.copy_count(), 5);
        assert_eq!(rp.diff_data_center_count, 2);
        assert_eq!(rp.diff_rack_count, 1);
        assert_eq!(rp.same_rack_count, 1);

        let rp: u8 = rp.into();
        assert_eq!(rp, 211);
    }
}
