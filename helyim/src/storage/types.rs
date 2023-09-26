use std::fmt::{Display, Formatter};

use crate::storage::needle::TOMBSTONE_FILE_SIZE;

pub type VolumeId = u32;
pub type NeedleId = u64;
pub type Cookie = u32;

#[derive(Copy, Clone)]
pub enum DiskType {
    // Hard disk drive,
    Hdd,
    // Solid state drive
    Ssd,
}

impl Display for DiskType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DiskType::Hdd => write!(f, "hard disk drive"),
            DiskType::Ssd => write!(f, "solid state drive"),
        }
    }
}

pub fn needle_is_deleted(size: u32) -> bool {
    size == TOMBSTONE_FILE_SIZE.wrapping_neg() as u32
}
