use std::{
    cmp::Ordering,
    fmt::{Display, Formatter},
};

use crate::storage::needle::{
    NEEDLE_CHECKSUM_SIZE, NEEDLE_HEADER_SIZE, NEEDLE_PADDING_SIZE, TOMBSTONE_FILE_SIZE,
};

macro_rules! def_needle_type {
    ($type_name:ident, $typ:ty) => {
        #[derive(Copy, Clone, Default, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
        pub struct $type_name(pub $typ);

        impl Display for $type_name {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl PartialEq<$typ> for $type_name {
            fn eq(&self, other: &$typ) -> bool {
                self.0 == *other
            }
        }

        impl PartialOrd<$typ> for $type_name {
            fn partial_cmp(&self, other: &$typ) -> Option<Ordering> {
                self.0.partial_cmp(other)
            }
        }
    };
}

pub type VolumeId = u32;
pub type NeedleId = u64;

def_needle_type!(Offset, u32);

impl Offset {
    pub fn actual_offset(&self) -> u64 {
        (self.0 * NEEDLE_PADDING_SIZE) as u64
    }
}

def_needle_type!(Size, i32);

impl Size {
    pub fn is_deleted(&self) -> bool {
        self.0 < 0 || self.0 == TOMBSTONE_FILE_SIZE
    }

    pub fn padding_len(&self) -> u32 {
        NEEDLE_PADDING_SIZE
            - ((NEEDLE_HEADER_SIZE + self.0 as u32 + NEEDLE_CHECKSUM_SIZE) % NEEDLE_PADDING_SIZE)
    }

    pub fn actual_size(&self) -> u64 {
        (NEEDLE_HEADER_SIZE + self.0 as u32 + NEEDLE_CHECKSUM_SIZE + self.padding_len()) as u64
    }
}

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
