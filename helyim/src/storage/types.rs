use std::fmt::{Display, Formatter};

pub type VolumeId = u32;
pub type NeedleId = u64;

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
