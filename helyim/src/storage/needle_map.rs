use std::{
    fs::File,
    io::{BufReader, Read, Write},
};

use bytes::{Buf, BufMut};
use tracing::debug;

use crate::{
    errors::Result,
    storage::{
        needle::NeedleValue,
        needle_value_map::{MemoryNeedleValueMap, NeedleValueMap},
    },
};

#[derive(Copy, Clone, Debug, Default)]
pub enum NeedleMapType {
    #[default]
    NeedleMapInMemory = 0,
}

#[derive(Default)]
struct Metric {
    maximum_file_key: u64,
    file_count: u64,
    deleted_count: u64,
    deleted_bytes: u64,
    file_bytes: u64,
}

pub struct NeedleMapper {
    needle_value_map: Box<dyn NeedleValueMap>,
    index_file: Option<File>,
    metric: Metric,
}

impl Default for NeedleMapper {
    fn default() -> Self {
        NeedleMapper {
            needle_value_map: Box::new(MemoryNeedleValueMap::new()),
            metric: Metric::default(),
            index_file: None,
        }
    }
}

impl NeedleMapper {
    pub fn new(kind: NeedleMapType) -> NeedleMapper {
        #[allow(unreachable_patterns)]
        match kind {
            NeedleMapType::NeedleMapInMemory => NeedleMapper {
                needle_value_map: Box::new(MemoryNeedleValueMap::new()),
                ..Default::default()
            },
            _ => panic!("not support map type: {:?}", kind),
        }
    }

    pub fn load_idx_file(&mut self, index_file: File) -> Result<()> {
        let mut last_offset = 0;
        let mut last_size = 0;
        walk_index_file(&index_file, |key, offset, size| -> Result<()> {
            if offset > last_offset {
                last_offset = offset;
                last_size = size;
            }

            if offset > 0 {
                self.set(key, NeedleValue { offset, size });
            } else {
                self.delete(key);
            }
            Ok(())
        })?;
        self.index_file = Some(index_file);
        Ok(())
    }

    pub fn set(&mut self, key: u64, index: NeedleValue) -> Option<NeedleValue> {
        debug!("needle map set key: {}, {:?}", key, index);
        if key > self.metric.maximum_file_key {
            self.metric.maximum_file_key = key;
        }
        self.metric.file_count += 1;
        self.metric.file_bytes += index.size as u64;
        let old = self.needle_value_map.set(key, index);

        if let Some(n) = old {
            self.metric.deleted_count += 1;
            self.metric.deleted_bytes += n.size as u64;
        }

        old
    }

    pub fn delete(&mut self, key: u64) -> Option<NeedleValue> {
        let deleted = self.needle_value_map.delete(key);

        if let Some(n) = deleted {
            self.metric.deleted_count += 1;
            self.metric.deleted_bytes += n.size as u64;
        }

        debug!("needle map delete key: {} {:?}", key, deleted);
        deleted
    }

    pub fn get(&self, key: u64) -> Option<NeedleValue> {
        self.needle_value_map.get(key)
    }

    pub fn file_count(&self) -> u64 {
        self.metric.file_count
    }

    pub fn deleted_count(&self) -> u64 {
        self.metric.deleted_count
    }

    pub fn deleted_bytes(&self) -> u64 {
        self.metric.deleted_bytes
    }

    pub fn max_file_key(&self) -> u64 {
        self.metric.maximum_file_key
    }

    pub fn content_size(&self) -> u64 {
        self.metric.file_bytes
    }

    pub fn index_file_size(&self) -> Result<u64> {
        let size = match self.index_file.as_ref() {
            Some(file) => file.metadata()?.len(),
            None => 0,
        };
        Ok(size)
    }

    pub fn append_to_index_file(&mut self, key: u64, value: NeedleValue) -> Result<()> {
        if let Some(file) = self.index_file.as_mut() {
            let mut buf = vec![];
            buf.put_u64(key);
            buf.put_u32(value.offset);
            buf.put_u32(value.size);

            file.write_all(&buf)?;
        }

        Ok(())
    }
}

pub fn index_entry(mut buf: &[u8]) -> (u64, u32, u32) {
    let key = buf.get_u64();
    let offset = buf.get_u32();
    let size = buf.get_u32();

    (key, offset, size)
}

// walks through index file, call fn(key, offset, size), stop with error returned by fn
pub fn walk_index_file<T>(f: &File, mut walk: T) -> Result<()>
where
    T: FnMut(u64, u32, u32) -> Result<()>,
{
    let mut reader = BufReader::new(f.try_clone()?);
    let mut buf: Vec<u8> = vec![0; 16];

    // if there is a not complete entry, will err
    for _ in 0..(f.metadata()?.len() + 15) / 16 {
        reader.read_exact(&mut buf)?;

        let (key, offset, size) = index_entry(&buf);
        walk(key, offset, size)?;
    }

    Ok(())
}
