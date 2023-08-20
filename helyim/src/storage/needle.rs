#![allow(dead_code)]

use std::{
    fmt::{Display, Formatter},
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
};

use bytes::{Buf, BufMut, Bytes};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::{
    anyhow,
    errors::{Error, Result},
    storage::{
        crc,
        ttl::Ttl,
        version::{Version, CURRENT_VERSION, VERSION2},
    },
};

pub const NEEDLE_HEADER_SIZE: u32 = 16;
pub const NEEDLE_PADDING_SIZE: u32 = 8;
pub const NEEDLE_CHECKSUM_SIZE: u32 = 4;
pub const MAX_POSSIBLE_VOLUME_SIZE: u64 = 4 * 1024 * 1024 * 1024 * 8;
pub const PAIR_NAME_PREFIX: &str = "Helyim-";
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

pub const NEEDLE_FLAG_OFFSET: usize = 20;
pub const NEEDLE_ID_OFFSET: usize = 4;
pub const NEEDLE_SIZE_OFFSET: usize = 12;

/// Needle index
#[derive(Debug, Copy, Clone)]
pub struct NeedleValue {
    // pub key: u64,
    /// needle offset in the store
    pub offset: u32,
    /// needle data size
    pub size: u32,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Needle {
    pub cookie: u32,
    pub id: u64,
    pub size: u32,

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

pub fn get_actual_size(size: u32) -> u32 {
    let left = (NEEDLE_HEADER_SIZE + size + NEEDLE_CHECKSUM_SIZE) % NEEDLE_PADDING_SIZE;
    let padding = if left > 0 {
        NEEDLE_PADDING_SIZE - left
    } else {
        0
    };

    NEEDLE_HEADER_SIZE + size + NEEDLE_CHECKSUM_SIZE + padding
}

pub fn true_offset(offset: u32) -> u64 {
    (offset * NEEDLE_PADDING_SIZE) as u64
}

fn read_needle_blob(file: &mut File, offset: u32, size: u32) -> Result<Bytes> {
    let size = get_actual_size(size);
    let mut buffer = vec![0; size as usize];

    let offset = true_offset(offset);
    file.seek(SeekFrom::Start(offset))?;

    file.read_exact(&mut buffer)?;
    Ok(Bytes::from(buffer))
}

impl Needle {
    pub fn parse_needle_header(&mut self, mut bytes: &[u8]) {
        self.cookie = bytes.get_u32();
        self.id = bytes.get_u64();
        self.size = bytes.get_u32();
        debug!(
            "parse needle header success, cookie: {}, id: {}, size: {}",
            self.cookie, self.id, self.size
        );
    }

    pub fn replicate_serialize(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }

    pub fn replicate_deserialize(data: &[u8]) -> Result<Needle> {
        bincode::deserialize(data).map_err(Error::from)
    }

    pub fn parse_path(&mut self, fid: &str) -> Result<()> {
        if fid.len() <= 8 {
            return Err(anyhow!("invalid fid: {}", fid));
        }

        let (id, delta) = match fid.find('_') {
            Some(idx) => (&fid[0..idx], &fid[idx + 1..]),
            None => (&fid[0..fid.len()], &fid[0..0]),
        };

        let ret = parse_key_hash(id)?;
        self.id = ret.0;
        self.cookie = ret.1;
        if !delta.is_empty() {
            let idelta: u64 = delta.parse()?;
            self.id += idelta;
        }

        Ok(())
    }

    fn read_needle_data(&mut self, bytes: Bytes) {
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
            self.ttl = Ttl::from(&bytes[idx..idx + TTL_BYTES_LENGTH]);
            idx += TTL_BYTES_LENGTH;
        }

        if idx < len && self.has_pairs() {
            self.pairs_size = (&bytes[idx..idx + 2]).get_u16();
            idx += 2;
            self.pairs = bytes.slice(idx..idx + self.pairs_size as usize);
        }
    }

    pub fn append<W: Write>(&mut self, w: &mut W, version: Version) -> Result<(u32, u32)> {
        if version != CURRENT_VERSION {
            return Err(anyhow!("no supported version"));
        }

        self.data_size = self.data.len() as u32;
        self.name_size = self.name.len() as u8;
        self.mime_size = self.mime.len() as u8;
        self.pairs_size = self.pairs.len() as u16;

        let mut buf = vec![];
        buf.put_u32(self.cookie);
        buf.put_u64(self.id);
        // placeholder for self.size
        buf.put_u32(0);

        if self.data_size > 0 {
            buf.put_u32(self.data_size);
            buf.put_slice(&self.data);
            buf.put_u8(self.flags);
            self.size = 4 + self.data_size + 1; // one for flag;
            if self.has_name() {
                buf.put_u8(self.name_size);
                buf.put_slice(&self.name);
                self.size += 1 + self.name_size as u32;
            }
            if self.has_mime() {
                buf.put_u8(self.mime_size);
                buf.put_slice(&self.mime);
                self.size += 1 + self.mime_size as u32;
            }
            if self.has_last_modified_date() {
                buf.put_u64(self.last_modified);
                self.size += LAST_MODIFIED_BYTES_LENGTH as u32;
            }
            if self.has_ttl() {
                buf.put_slice(&self.ttl.as_bytes());
                self.size += TTL_BYTES_LENGTH as u32;
            }
            if self.has_pairs() {
                buf.put_u16(self.pairs.len() as u16);
                buf.put_slice(&self.pairs);
                self.size += 2 + self.pairs.len() as u32;
            }
        } else {
            self.size = 0
        }
        (&mut buf[12..16]).put_u32(self.size);

        let mut padding = 0;
        if (NEEDLE_HEADER_SIZE + self.size + NEEDLE_CHECKSUM_SIZE) % NEEDLE_PADDING_SIZE != 0 {
            padding = NEEDLE_PADDING_SIZE
                - (NEEDLE_HEADER_SIZE + self.size + NEEDLE_CHECKSUM_SIZE) % NEEDLE_PADDING_SIZE;
        }

        buf.put_u32(self.checksum);
        buf.put_slice(&vec![0; padding as usize]);
        w.write_all(&buf)?;

        Ok((self.data_size, get_actual_size(self.size)))
    }

    pub fn read_data(
        &mut self,
        file: &mut File,
        offset: u32,
        size: u32,
        version: Version,
    ) -> Result<()> {
        let bytes = read_needle_blob(file, offset, size)?;
        self.parse_needle_header(&bytes);

        if self.size != size {
            return Err(anyhow!(
                "file entry not found. needle {} memory {}",
                self.size,
                size
            ));
        }

        if version == VERSION2 {
            let end = (NEEDLE_HEADER_SIZE + self.size) as usize;
            self.read_needle_data(bytes.slice(NEEDLE_HEADER_SIZE as usize..end));
        }

        let checksum_start = (NEEDLE_HEADER_SIZE + size) as usize;
        let checksum_end = (NEEDLE_HEADER_SIZE + size + NEEDLE_CHECKSUM_SIZE) as usize;
        self.checksum = (&bytes[checksum_start..checksum_end]).get_u32();
        let checksum = crc::checksum(&self.data);

        if self.checksum != checksum {
            return Err(anyhow!(
                "CRC error, read: {}, calculate: {}, may be data on disk corrupted",
                self.checksum,
                checksum
            ));
        }

        Ok(())
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
        self.flags |= FLAG_IS_DELETE
    }

    pub fn etag(&self) -> String {
        let mut buf: Vec<u8> = vec![0; 4];
        buf.put_u32(self.checksum);
        format!("{}{}{}{}", buf[0], buf[1], buf[2], buf[3])
    }
}

fn parse_key_hash(hash: &str) -> Result<(u64, u32)> {
    if hash.len() <= 8 || hash.len() > 24 {
        return Err(anyhow!("key hash too short or too long: {}", hash));
    }

    let key_end = hash.len() - 8;

    let key: u64 = u64::from_str_radix(&hash[0..key_end], 16)?;
    let cookie: u32 = u32::from_str_radix(&hash[key_end..], 16)?;

    Ok((key, cookie))
}
