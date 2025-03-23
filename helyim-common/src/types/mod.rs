use std::fmt::{Display, Formatter};

mod file_id;
pub use file_id::FileId;
mod needle;
pub use needle::{NeedleId, NeedleValue, Offset, Size, read_index_entry, walk_index_file};

mod volume;
pub use volume::{ReplicaPlacement, SuperBlock, VolumeId};

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
