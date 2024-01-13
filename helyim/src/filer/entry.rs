use std::{
    ops::{Deref, DerefMut},
    time::SystemTime,
};

use faststr::FastStr;
use helyim_proto::filer::FileChunk;
use rustix::process::{getgid, getuid};

use crate::filer::file_chunks::total_size;

#[derive(Clone)]
pub struct Attr {
    mtime: SystemTime,
    crtime: SystemTime,
    mode: u32,
    uid: u32,
    gid: u32,
    mime: FastStr,
    replication: FastStr,
    collection: FastStr,
    ttl: i32,
}

impl Attr {
    pub fn root_path(time: SystemTime) -> Self {
        Self {
            mtime: time,
            crtime: time,
            mode: (1 << 31) | 0o755,
            uid: getuid().as_raw(),
            gid: getgid().as_raw(),
            mime: FastStr::empty(),
            replication: FastStr::empty(),
            collection: FastStr::empty(),
            ttl: 0,
        }
    }

    pub fn is_directory(&self) -> bool {
        self.mode & (1 << 31) > 0
    }
}

#[derive(Clone)]
pub struct Entry {
    pub path: FastStr,
    pub attr: Attr,
    pub chunks: Vec<FileChunk>,
}

impl Entry {
    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn size(&self) -> u64 {
        total_size(&self.chunks)
    }

    pub fn timestamp(&self) -> SystemTime {
        if self.is_directory() {
            self.crtime
        } else {
            self.mtime
        }
    }
}

impl Deref for Entry {
    type Target = Attr;

    fn deref(&self) -> &Self::Target {
        &self.attr
    }
}

impl DerefMut for Entry {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.attr
    }
}
