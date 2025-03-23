use std::{
    ops::{Deref, DerefMut},
    time::SystemTime,
};

use faststr::FastStr;
use helyim_common::time::{TimeError, get_time, timestamp_to_time};
use helyim_proto::filer::{FileChunk, FuseAttributes};
use rustix::process::{getgid, getuid};
use serde::{Deserialize, Serialize};

use crate::file_chunk::total_size;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Attr {
    pub mtime: SystemTime,
    pub crtime: SystemTime,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub mime: FastStr,
    pub replication: FastStr,
    pub collection: FastStr,
    pub ttl: i32,
    pub username: FastStr,
    pub group_names: Vec<FastStr>,
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
            username: FastStr::empty(),
            group_names: vec![],
        }
    }

    pub fn is_directory(&self) -> bool {
        self.mode & (1 << 31) > 0
    }
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    pub full_path: String,
    pub attr: Attr,
    pub chunks: Vec<FileChunk>,
}

impl Entry {
    pub fn new<S: ToString>(path: S) -> Entry {
        Self {
            full_path: path.to_string(),
            attr: Attr {
                mtime: SystemTime::now(),
                crtime: SystemTime::now(),
                mode: 0o644,
                uid: getuid().as_raw(),
                gid: getgid().as_raw(),
                mime: Default::default(),
                replication: Default::default(),
                collection: Default::default(),
                ttl: 0,
                username: Default::default(),
                group_names: vec![],
            },
            chunks: vec![],
        }
    }
    pub fn path(&self) -> &str {
        &self.full_path
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

pub fn entry_attr_to_pb(entry: &Entry) -> Result<FuseAttributes, TimeError> {
    Ok(FuseAttributes {
        file_size: 0,
        crtime: get_time(entry.attr.crtime)?.as_millis() as i64,
        mtime: get_time(entry.attr.mtime)?.as_millis() as i64,
        file_mode: entry.attr.mode,
        uid: entry.uid,
        gid: entry.gid,
        mime: entry.mime.to_string(),
        replication: entry.attr.replication.to_string(),
        collection: entry.attr.collection.to_string(),
        ttl_sec: entry.attr.ttl,
        user_name: entry.attr.username.to_string(),
        group_name: entry
            .group_names
            .iter()
            .map(|name| name.to_string())
            .collect(),
    })
}

pub fn pb_to_entry_attr(attr: &FuseAttributes) -> Result<Attr, TimeError> {
    Ok(Attr {
        mtime: timestamp_to_time(attr.mtime as u64)?,
        crtime: timestamp_to_time(attr.crtime as u64)?,
        mode: attr.file_mode,
        uid: attr.uid,
        gid: attr.gid,
        mime: FastStr::new(&attr.mime),
        replication: FastStr::new(&attr.replication),
        collection: FastStr::new(&attr.collection),
        ttl: attr.ttl_sec,
        username: FastStr::new(&attr.user_name),
        group_names: attr.group_name.iter().map(FastStr::new).collect(),
    })
}
