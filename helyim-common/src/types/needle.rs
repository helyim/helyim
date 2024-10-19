use std::{
    fmt::{Display, Formatter},
    fs::File,
    io,
    io::{BufReader, Read},
};

use bytes::{Buf, BufMut};

use crate::consts::{
    NEEDLE_CHECKSUM_SIZE, NEEDLE_HEADER_SIZE, NEEDLE_PADDING_SIZE, TOMBSTONE_FILE_SIZE,
};
macro_rules! def_needle_type {
    ($type_name:ident, $typ:ty) => {
        #[derive(Copy, Clone, Default, serde::Serialize, serde::Deserialize, Eq, PartialEq)]
        pub struct $type_name(pub $typ);

        impl std::fmt::Display for $type_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl std::fmt::Debug for $type_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl PartialEq<$typ> for $type_name {
            fn eq(&self, other: &$typ) -> bool {
                self.0 == *other
            }
        }

        impl PartialOrd<$typ> for $type_name {
            fn partial_cmp(&self, other: &$typ) -> Option<std::cmp::Ordering> {
                self.0.partial_cmp(other)
            }
        }
    };
}

pub type NeedleId = u64;

def_needle_type!(Offset, u32);

impl Offset {
    pub fn actual_offset(&self) -> u64 {
        (self.0 * NEEDLE_PADDING_SIZE) as u64
    }
}

impl From<u64> for Offset {
    fn from(value: u64) -> Self {
        Self(value as u32 / NEEDLE_PADDING_SIZE)
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

/// Needle index
#[derive(Copy, Clone, Debug)]
pub struct NeedleValue {
    /// needle offset
    ///
    /// in data file, the real offset is `offset * NEEDLE_PADDING_SIZE`
    pub offset: Offset,
    /// needle data size
    pub size: Size,
}

impl leapfrog::Value for NeedleValue {
    fn is_redirect(&self) -> bool {
        self.offset.0.is_redirect() && self.size.0.is_redirect()
    }

    fn is_null(&self) -> bool {
        self.offset.0.is_null() && self.size.0.is_null()
    }

    fn redirect() -> Self {
        Self {
            offset: Offset(u32::redirect()),
            size: Size(i32::redirect()),
        }
    }

    fn null() -> Self {
        Self {
            offset: Offset(u32::null()),
            size: Size(i32::null()),
        }
    }
}

impl NeedleValue {
    pub fn deleted() -> Self {
        Self {
            offset: Offset(0),
            size: Size(-1),
        }
    }
    pub fn as_bytes(&self, needle_id: NeedleId) -> Vec<u8> {
        let mut buf = vec![];
        buf.put_u64(needle_id);
        buf.put_u32(self.offset.0);
        buf.put_i32(self.size.0);
        buf
    }
}

impl Display for NeedleValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "(offset: {}, size: {})", self.offset, self.size)
    }
}

pub fn read_index_entry(mut buf: &[u8]) -> (NeedleId, Offset, Size) {
    let key = buf.get_u64();
    let offset = Offset(buf.get_u32());
    let size = Size(buf.get_i32());
    (key, offset, size)
}

// walks through index file, call fn(key, offset, size), stop with error returned by fn
pub fn walk_index_file<T>(f: &mut File, mut walk: T) -> Result<(), io::Error>
where
    T: FnMut(NeedleId, Offset, Size) -> Result<(), io::Error>,
{
    let len = f.metadata()?.len();
    let mut reader = BufReader::new(f);
    let mut buf: Vec<u8> = vec![0; 16];

    // if there is a not complete entry, will err
    for _ in 0..(len + 15) / 16 {
        reader.read_exact(&mut buf)?;

        let (key, offset, size) = read_index_entry(&buf);
        walk(key, offset, size)?;
    }

    Ok(())
}
