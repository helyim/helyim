use std::{
    fmt::{Display, Formatter},
    fs::File,
    os::unix::fs::FileExt,
};

use bytes::{Buf, BufMut, Bytes};
use serde::{Deserialize, Serialize};
use tracing::{debug, error};

use crate::storage::{
    crc,
    ttl::Ttl,
    types::{Cookie, Offset, Size},
    version::{Version, CURRENT_VERSION, VERSION2},
    NeedleId, VolumeId,
};

mod metric;

mod needle_map;
pub use needle_map::{read_index_entry, walk_index_file, NeedleMapType, NeedleMapper};

mod needle_value_map;
pub use needle_value_map::{MemoryNeedleValueMap, NeedleValueMap, SortedIndexMap};

use crate::storage::ttl::TtlError;

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

#[derive(Default, Serialize, Deserialize)]
pub struct Needle {
    pub cookie: Cookie,
    pub id: NeedleId,
    pub size: Size,
    pub data_size: u32,
    pub data: Bytes,
    pub flags: u8,
    pub name_size: u8,
    pub name: Bytes,
    pub mime_size: u8,
    pub mime: Bytes,
    pub pairs_size: u16,
    pub pairs: Bytes,
    pub last_modified: u64,
    pub ttl: Ttl,
    pub checksum: u32,
    pub padding: Vec<u8>,
}

impl Display for Needle {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "id: {}, cookie: {} size: {}, data_len: {}, has_pairs: {} flag: {}",
            self.id,
            self.cookie,
            self.size,
            self.data.len(),
            self.has_pairs(),
            self.flags,
        )
    }
}

pub fn read_needle_blob(file: &File, offset: Offset, size: Size) -> Result<Bytes, NeedleError> {
    let size = size.actual_size();
    let mut buf = vec![0; size as usize];

    let offset = offset.actual_offset();
    file.read_exact_at(&mut buf, offset)?;
    Ok(Bytes::from(buf))
}

impl Needle {
    pub fn new_with_fid(fid: &str) -> Result<Self, NeedleError> {
        let mut needle = Self::default();
        needle.parse_path(fid)?;
        Ok(needle)
    }

    pub fn parse_needle_header(&mut self, mut bytes: &[u8]) {
        self.cookie = bytes.get_u32();
        self.id = bytes.get_u64();
        self.size = Size(bytes.get_i32());
        debug!(
            "parse needle header success, cookie: {}, id: {}, size: {}",
            self.cookie, self.id, self.size
        );
    }

    pub fn parse_path(&mut self, fid: &str) -> Result<(), NeedleError> {
        if fid.len() <= 8 {
            return Err(NeedleError::InvalidFid(fid.to_string()));
        }

        let (id, delta) = match fid.find('_') {
            Some(idx) => (&fid[0..idx], &fid[idx + 1..]),
            None => (&fid[0..fid.len()], &fid[0..0]),
        };

        let (key, cookie) = parse_key_hash(id)?;
        self.id = key;
        self.cookie = cookie;
        if !delta.is_empty() {
            let id_delta: u64 = delta.parse()?;
            self.id += id_delta;
        }

        Ok(())
    }

    pub fn read_needle_body(
        &mut self,
        data_file: &File,
        offset: u64,
        body_len: u32,
        version: Version,
    ) -> Result<(), NeedleError> {
        if body_len == 0 {
            return Ok(());
        }
        match version {
            VERSION2 => {
                let mut buf = vec![0u8; body_len as usize];
                data_file.read_exact_at(&mut buf, offset)?;
                self.read_needle_data(Bytes::from(buf))?;
                self.checksum = crc::checksum(&self.data);
            }
            n => return Err(NeedleError::UnsupportedVersion(n)),
        }
        Ok(())
    }

    pub fn read_needle_data(&mut self, bytes: Bytes) -> Result<(), NeedleError> {
        let mut idx = 0;
        let len = bytes.len();

        if idx < len {
            self.data_size = (&bytes[idx..idx + 4]).get_u32();
            idx += 4;

            self.data = bytes.slice(idx..idx + self.data_size as usize);
            idx += self.data_size as usize;
            self.flags = bytes[idx];
            idx += 1;
        }

        if idx < len && self.has_name() {
            self.name_size = bytes[idx];
            idx += 1;
            self.name = bytes.slice(idx..idx + self.name_size as usize);
            idx += self.name_size as usize;
        }

        if idx < len && self.has_mime() {
            self.mime_size = bytes[idx];
            idx += 1;
            self.mime = bytes.slice(idx..idx + self.mime_size as usize);
            idx += self.mime_size as usize;
        }

        if idx < len && self.has_last_modified_date() {
            self.last_modified = (&bytes[idx..idx + LAST_MODIFIED_BYTES_LENGTH]).get_u64();
            idx += LAST_MODIFIED_BYTES_LENGTH;
        }

        if idx < len && self.has_ttl() {
            self.ttl = Ttl::from_bytes(&bytes[idx..idx + TTL_BYTES_LENGTH])?;
            idx += TTL_BYTES_LENGTH;
        }

        if idx < len && self.has_pairs() {
            self.pairs_size = (&bytes[idx..idx + 2]).get_u16();
            idx += 2;
            self.pairs = bytes.slice(idx..idx + self.pairs_size as usize);
        }

        Ok(())
    }

    pub fn append<W: FileExt>(
        &mut self,
        w: &W,
        offset: u64,
        version: Version,
    ) -> Result<(), NeedleError> {
        if version != CURRENT_VERSION {
            return Err(NeedleError::UnsupportedVersion(version));
        }

        self.data_size = self.data.len() as u32;
        self.name_size = self.name.len() as u8;
        self.mime_size = self.mime.len() as u8;
        self.pairs_size = self.pairs.len() as u16;
        self.size = Size(0);

        let mut buf = vec![];
        buf.put_u32(self.cookie);
        buf.put_u64(self.id);
        buf.put_i32(self.size.0);

        if self.data_size > 0 {
            buf.put_u32(self.data_size);
            buf.put_slice(&self.data);
            buf.put_u8(self.flags);
            self.size.0 = 4 + self.data_size as i32 + 1; // one for flag;
            if self.has_name() {
                buf.put_u8(self.name_size);
                buf.put_slice(&self.name);
                self.size.0 += 1 + self.name_size as i32;
            }
            if self.has_mime() {
                buf.put_u8(self.mime_size);
                buf.put_slice(&self.mime);
                self.size.0 += 1 + self.mime_size as i32;
            }
            if self.has_last_modified_date() {
                buf.put_u64(self.last_modified);
                self.size.0 += LAST_MODIFIED_BYTES_LENGTH as i32;
            }
            if self.has_ttl() {
                buf.put_slice(&self.ttl.as_bytes());
                self.size.0 += TTL_BYTES_LENGTH as i32;
            }
            if self.has_pairs() {
                buf.put_u16(self.pairs.len() as u16);
                buf.put_slice(&self.pairs);
                self.size.0 += 2 + self.pairs.len() as i32;
            }
        }
        if self.size > 0 {
            (&mut buf[12..16]).put_u32(self.size.0 as u32);
        }

        buf.put_u32(self.checksum);

        let padding = self.size.padding_len();
        buf.put_slice(&vec![0; padding as usize]);
        w.write_all_at(&buf, offset)?;

        Ok(())
    }

    pub fn read_bytes(
        &mut self,
        bytes: Bytes,
        offset: Offset,
        size: Size,
        version: Version,
    ) -> Result<(), NeedleError> {
        self.parse_needle_header(&bytes);

        if self.size != size && offset.actual_offset() < MAX_POSSIBLE_VOLUME_SIZE {
            return Err(NeedleError::SizeNotMatch(self.size, size));
        }

        if version == VERSION2 {
            let end = NEEDLE_HEADER_SIZE + self.size.0 as u32;
            self.read_needle_data(bytes.slice(NEEDLE_HEADER_SIZE as usize..end as usize))?;
        }

        let checksum_start = NEEDLE_HEADER_SIZE + size.0 as u32;
        let checksum_end = (NEEDLE_HEADER_SIZE + size.0 as u32 + NEEDLE_CHECKSUM_SIZE) as usize;
        self.checksum = (&bytes[checksum_start as usize..checksum_end]).get_u32();
        let checksum = crc::checksum(&self.data);

        if self.checksum != checksum {
            return Err(NeedleError::Crc(self.checksum, checksum));
        }

        Ok(())
    }

    pub fn read_data(
        &mut self,
        file: &File,
        offset: Offset,
        size: Size,
        version: Version,
    ) -> Result<(), NeedleError> {
        let bytes = read_needle_blob(file, offset, size)?;
        self.read_bytes(bytes, offset, size, version)
    }

    pub fn has_ttl(&self) -> bool {
        self.flags & FLAG_HAS_TTL > 0
    }

    pub fn set_has_ttl(&mut self) {
        self.flags |= FLAG_HAS_TTL
    }

    pub fn has_name(&self) -> bool {
        self.flags & FLAG_HAS_NAME > 0
    }

    pub fn set_name(&mut self) {
        self.flags |= FLAG_HAS_NAME
    }

    pub fn has_mime(&self) -> bool {
        self.flags & FLAG_HAS_MIME > 0
    }

    pub fn set_has_mime(&mut self) {
        self.flags |= FLAG_HAS_MIME
    }

    pub fn is_gzipped(&self) -> bool {
        self.flags & FLAG_GZIP > 0
    }

    pub fn set_gzipped(&mut self) {
        self.flags |= FLAG_GZIP
    }

    pub fn has_pairs(&self) -> bool {
        self.flags & FLAG_HAS_PAIRS > 0
    }

    pub fn set_has_pairs(&mut self) {
        self.flags |= FLAG_HAS_PAIRS
    }

    pub fn has_last_modified_date(&self) -> bool {
        self.flags & FLAG_HAS_LAST_MODIFIED_DATE > 0
    }

    pub fn set_has_last_modified_date(&mut self) {
        self.flags |= FLAG_HAS_LAST_MODIFIED_DATE
    }

    pub fn set_is_chunk_manifest(&mut self) {
        self.flags |= FLAG_IS_CHUNK_MANIFEST
    }

    pub fn is_chunk_manifest(&self) -> bool {
        self.flags & FLAG_IS_CHUNK_MANIFEST > 0
    }

    pub fn is_delete(&self) -> bool {
        self.flags & FLAG_IS_DELETE > 0
    }

    pub fn set_is_delete(&mut self) {
        self.flags |= FLAG_IS_DELETE;
    }

    pub fn etag(&self) -> String {
        let mut buf: Vec<u8> = vec![0; 4];
        buf.put_u32(self.checksum);
        format!("{}{}{}{}", buf[0], buf[1], buf[2], buf[3])
    }

    pub fn disk_size(&self) -> u64 {
        self.size.actual_size()
    }

    pub fn data_size(&self) -> usize {
        self.data.len()
    }

    pub fn body_len(&self) -> u32 {
        let padding = self.size.padding_len();
        self.size.0 as u32 + NEEDLE_CHECKSUM_SIZE + padding
    }
}

#[derive(thiserror::Error, Debug)]
pub enum NeedleError {
    #[error("Io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("error: {0}")]
    Box(#[from] Box<dyn std::error::Error + Sync + Send>),
    #[error("Parse integer error: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error("Ttl error: {0}")]
    Ttl(#[from] TtlError),

    #[error("Volume {0}: needle {1} has deleted.")]
    Deleted(VolumeId, u64),
    #[error("Volume {0}: needle {1} has expired.")]
    Expired(VolumeId, u64),
    #[error("Needle {0} not found.")]
    NotFound(u64),
    #[error("Cookie not match, needle cookie is {0} but got {1}")]
    CookieNotMatch(u32, u32),
    #[error("Size not match, needle size is {0} but got {1}")]
    SizeNotMatch(Size, Size),
    #[error("Unsupported version: {0}")]
    UnsupportedVersion(Version),
    #[error("Crc error, read: {0}, calculate: {1}, may be data on disk corrupted")]
    Crc(u32, u32),
    #[error("Invalid file id: {0}")]
    InvalidFid(String),
    #[error("key hash: {0} is too short or too long")]
    InvalidKeyHash(String),
}

impl From<NeedleError> for tonic::Status {
    fn from(value: NeedleError) -> Self {
        tonic::Status::internal(value.to_string())
    }
}

fn parse_key_hash(hash: &str) -> Result<(NeedleId, Cookie), NeedleError> {
    if hash.len() <= 8 || hash.len() > 24 {
        return Err(NeedleError::InvalidKeyHash(hash.to_string()));
    }

    let key_end = hash.len() - 8;

    let key: u64 = u64::from_str_radix(&hash[0..key_end], 16)?;
    let cookie: u32 = u32::from_str_radix(&hash[key_end..], 16)?;

    Ok((key, cookie))
}

pub fn read_needle_header(
    file: &File,
    version: Version,
    offset: u64,
) -> Result<(Needle, u32), NeedleError> {
    let mut needle = Needle::default();
    let mut body_len = 0;

    if version == VERSION2 {
        let mut buf = vec![0u8; NEEDLE_ENTRY_SIZE as usize];
        file.read_exact_at(&mut buf, offset)?;
        needle.parse_needle_header(&buf);
        body_len = needle.body_len();
    }

    Ok((needle, body_len))
}

#[cfg(test)]
mod tests {
    use crate::storage::needle::parse_key_hash;

    #[test]
    pub fn test_parse_key_hash() {
        let (key, cookie) = parse_key_hash("4ed4c8116e41").unwrap();
        assert_eq!(key, 0x4ed4);
        assert_eq!(cookie, 0xc8116e41);

        let (key, cookie) = parse_key_hash("4ed401116e41").unwrap();
        assert_eq!(key, 0x4ed4);
        assert_eq!(cookie, 0x01116e41);

        let (key, cookie) = parse_key_hash("ed400116e41").unwrap();
        assert_eq!(key, 0xed4);
        assert_eq!(cookie, 0x00116e41);

        let (key, cookie) = parse_key_hash("fed4c8114ed4c811f0116e41").unwrap();
        assert_eq!(key, 0xfed4c8114ed4c811);
        assert_eq!(cookie, 0xf0116e41);

        // invalid character
        assert!(parse_key_hash("helloworld").is_err());
        // too long
        assert!(parse_key_hash("4ed4c8114ed4c8114ed4c8111").is_err());
        // too short
        assert!(parse_key_hash("4ed4c811").is_err());
    }
}
