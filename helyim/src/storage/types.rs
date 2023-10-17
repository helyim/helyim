use std::{
    cmp::Ordering,
    fmt::{Display, Formatter},
    ops::{AddAssign, Deref},
};

use crate::storage::needle::TOMBSTONE_FILE_SIZE;

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

        impl AddAssign<$typ> for $type_name {
            fn add_assign(&mut self, rhs: $typ) {
                self.0 += rhs;
            }
        }

        impl Deref for $type_name {
            type Target = $typ;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
    };
}

pub type VolumeId = u32;
pub type NeedleId = u64;
pub type Offset = u32;

def_needle_type!(Size, i32);

impl Size {
    pub fn is_deleted(&self) -> bool {
        self.0 < 0 || self.0 == TOMBSTONE_FILE_SIZE
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
