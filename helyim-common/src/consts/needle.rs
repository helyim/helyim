pub const TOMBSTONE_FILE_SIZE: i32 = -1;
pub const NEEDLE_HEADER_SIZE: u32 = 16;
pub const NEEDLE_PADDING_SIZE: u32 = 8;
pub const NEEDLE_ID_SIZE: u32 = 8;
pub const OFFSET_SIZE: u32 = 4;
pub const SIZE_SIZE: u32 = 4;
// pub const TIMESTAMP_SIZE: u32 = 8;
pub const NEEDLE_ENTRY_SIZE: u32 = NEEDLE_ID_SIZE + OFFSET_SIZE + SIZE_SIZE;
pub const NEEDLE_CHECKSUM_SIZE: u32 = 4;
pub const NEEDLE_INDEX_SIZE: u32 = 16;
pub const MAX_POSSIBLE_VOLUME_SIZE: u64 = 4 * 1024 * 1024 * 1024 * 8;
pub const PAIR_NAME_PREFIX: &str = "helyim-";
pub const FLAG_GZIP: u8 = 0x01;
pub const FLAG_HAS_NAME: u8 = 0x02;
pub const FLAG_HAS_MIME: u8 = 0x04;
pub const FLAG_HAS_LAST_MODIFIED_DATE: u8 = 0x08;
pub const FLAG_HAS_TTL: u8 = 0x10;
pub const FLAG_HAS_PAIRS: u8 = 0x20;
pub const FLAG_IS_DELETE: u8 = 0x40;
pub const FLAG_IS_CHUNK_MANIFEST: u8 = 0x80;

pub const LAST_MODIFIED_BYTES_LENGTH: usize = 8;
pub const TTL_BYTES_LENGTH: usize = 2;

// pub const NEEDLE_FLAG_OFFSET: usize = 20;
// pub const NEEDLE_ID_OFFSET: usize = 4;
// pub const NEEDLE_SIZE_OFFSET: usize = 12;
