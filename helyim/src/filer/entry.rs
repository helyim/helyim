use std::{
    ops::{Deref, DerefMut},
    time::SystemTime, collections::HashMap,
};

use bytes::Bytes;
use faststr::FastStr;
use helyim_proto::filer::{Entry as PbEntry, FileChunk, FuseAttributes};
use prost::Message;
use rustix::process::{getgid, getuid};

use super::FilerError;
use crate::{
    filer::file_chunks::total_size,
    util::time::{get_system_time, get_time},
};

#[derive(Clone, Debug)]
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
}

impl Default for Attr {
    fn default() -> Self {
        Self {
            mtime: SystemTime::now(),
            crtime: SystemTime::now(),
            mode: Default::default(),
            uid: Default::default(),
            gid: Default::default(),
            mime: Default::default(),
            replication: Default::default(),
            collection: Default::default(),
            ttl: Default::default(),
        }
    }
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

#[derive(Clone, Default, Debug)]
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

    pub fn ttl(&self) -> i32 {
        self.ttl
    }

    pub fn encode_attributes_and_chunks(&self) -> Result<Vec<u8>, FilerError> {
        let message = PbEntry {
            attributes: Some(entry_attribute_to_pb(self)),
            chunks: self.chunks.clone(),
            ..Default::default()
        };

        let mut buf = Vec::new();
        message
            .encode(&mut buf)
            .map_err(|err| FilerError::SerializationErr)?;
        Ok(buf)
    }

    pub fn decode_attributes_and_chunks(&mut self, data: Vec<u8>) -> Result<(), FilerError> {
        let entry =
            PbEntry::decode(Bytes::from(data)).map_err(|_err| FilerError::SerializationErr)?;

        self.attr = pb_to_entry_attribute(entry.attributes.unwrap());
        self.chunks = entry.chunks;
        Ok(())
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

pub fn entry_attribute_to_pb(entry: &Entry) -> FuseAttributes {
    FuseAttributes {
        mtime: get_time(entry.attr.mtime)
            .expect("SystemTime before UNIX EPOCH")
            .as_secs(),
        file_mode: entry.attr.mode,
        uid: entry.attr.uid,
        gid: entry.attr.gid,
        crtime: get_time(entry.attr.crtime)
            .expect("SystemTime before UNIX EPOCH")
            .as_secs(),
        mime: entry.attr.mime.to_string(),
        ttl_sec: entry.attr.ttl,
        collection: entry.attr.collection.to_string(),
        replication: entry.attr.replication.to_string(),
        ..Default::default()
    }
}

pub fn pb_to_entry_attribute(attr: FuseAttributes) -> Attr {
    Attr {
        mtime: get_system_time(attr.mtime),
        mode: attr.file_mode,
        uid: attr.uid,
        gid: attr.gid,
        crtime: get_system_time(attr.crtime),
        mime: attr.mime.into(),
        ttl: attr.ttl_sec,
        collection: attr.collection.into(),
        replication: attr.replication.into(),
    }
}

pub fn entry_to_pb(entry: &Entry) -> PbEntry {
    PbEntry {
        name: entry.path().into(),
        is_directory: entry.is_directory(),
        chunks: entry.chunks,
        attributes: Some(entry_attribute_to_pb(&entry)),
        extended: HashMap::default(),
        hard_link_id: Vec::new(),
        hard_link_counter: 0,
        content: Vec::new(),
        remote_entry: None,
        quota: 0,
    }
}

#[cfg(test)]
mod test {
    use std::time::SystemTime;

    use faststr::FastStr;

    use super::{Attr, Entry};
    use crate::filer::FilerError;

    #[test]
    fn test_encode_and_decode() -> Result<(), FilerError> {
        let path: FastStr = "/etc/ceph/ceph.conf".into();
        let entry = Entry {
            path: path.clone(),
            attr: Attr {
                mtime: SystemTime::now(),
                crtime: SystemTime::now(),
                mode: 0644,
                uid: 1,
                gid: 1,
                mime: "application/zip".into(),
                replication: "r".into(),
                collection: "c".into(),
                ttl: 30,
            },
            chunks: vec![],
        };
        println!("{:?}", entry);

        let data = entry.encode_attributes_and_chunks()?;

        let mut entry_t = Entry {
            path,
            ..Default::default()
        };
        entry_t.decode_attributes_and_chunks(data)?;

        println!("{:?}", entry_t);
        Ok(())
    }
}
