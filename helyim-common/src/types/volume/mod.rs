use std::{
    io::ErrorKind,
    sync::atomic::{AtomicU16, Ordering},
};

use bytes::{Buf, BufMut};
use tokio::io;

use crate::{
    consts::SUPER_BLOCK_SIZE,
    ttl::Ttl,
    version::{Version, CURRENT_VERSION},
};

mod replica_placement;
pub use replica_placement::ReplicaPlacement;

pub type VolumeId = u32;

#[derive(Debug)]
pub struct SuperBlock {
    pub version: Version,
    pub replica_placement: ReplicaPlacement,
    pub ttl: Ttl,
    pub compact_revision: AtomicU16,
}

impl Default for SuperBlock {
    fn default() -> Self {
        SuperBlock {
            version: CURRENT_VERSION,
            replica_placement: ReplicaPlacement::default(),
            ttl: Ttl::default(),
            compact_revision: AtomicU16::new(0),
        }
    }
}

impl SuperBlock {
    pub fn parse(buf: [u8; SUPER_BLOCK_SIZE]) -> Result<SuperBlock, io::Error> {
        let rp = ReplicaPlacement::from_u8(buf[1])?;
        let ttl = Ttl::from_bytes(&buf[2..4])
            .map_err(|err| io::Error::new(ErrorKind::InvalidData, format!("Ttl error: {err}")))?;
        let compact_revision = (&buf[4..6]).get_u16();
        let compact_revision = AtomicU16::new(compact_revision);
        Ok(SuperBlock {
            version: buf[0],
            replica_placement: rp,
            ttl,
            compact_revision,
        })
    }

    pub fn as_bytes(&self) -> [u8; SUPER_BLOCK_SIZE] {
        let mut buf = [0; SUPER_BLOCK_SIZE];
        buf[0] = self.version;
        buf[1] = self.replica_placement.into();

        let mut idx = 2;
        for u in self.ttl.as_bytes() {
            buf[idx] = u;
            idx += 1;
        }
        (&mut buf[4..6]).put_u16(self.compact_revision());
        buf
    }

    pub fn compact_revision(&self) -> u16 {
        self.compact_revision.load(Ordering::Relaxed)
    }

    pub fn add_compact_revision(&self, revision: u16) -> u16 {
        self.compact_revision.fetch_add(revision, Ordering::Relaxed)
    }
}
