use std::result::Result;

use bytes::{Buf, BufMut};
use futures::future::BoxFuture;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, error};

use crate::{
    storage::{
        needle::NeedleValue,
        needle_value_map::{MemoryNeedleValueMap, NeedleValueMap},
        types::{Offset, Size},
        NeedleError, NeedleId, VolumeError, VolumeId,
    },
    util::file,
};

#[derive(Copy, Clone, Debug, Default)]
pub enum NeedleMapType {
    #[default]
    NeedleMapInMemory = 0,
    PlaceHolder,
}

#[derive(Default)]
struct Metric {
    maximum_file_key: NeedleId,
    file_count: u64,
    deleted_count: u64,
    deleted_bytes: u64,
    file_bytes: u64,
}

pub struct NeedleMapper {
    volume_id: VolumeId,
    needle_value_map: Box<dyn NeedleValueMap>,
    index_file: Option<String>,
    metric: Metric,
}

impl Default for NeedleMapper {
    fn default() -> Self {
        NeedleMapper {
            volume_id: 0,
            needle_value_map: Box::new(MemoryNeedleValueMap::new()),
            metric: Metric::default(),
            index_file: None,
        }
    }
}

impl NeedleMapper {
    pub fn new(volume_id: VolumeId, kind: NeedleMapType) -> NeedleMapper {
        match kind {
            NeedleMapType::NeedleMapInMemory => NeedleMapper {
                needle_value_map: Box::new(MemoryNeedleValueMap::new()),
                volume_id,
                ..Default::default()
            },
            _ => panic!("not support map type: {:?}", kind),
        }
    }

    /// TODO: How to use walk_index_file() in a simply way
    pub async fn load_idx_file(&mut self, path: &str) -> Result<(), VolumeError> {
        let index_file = file::open(path).await?;
        let len = index_file.metadata().await?.len();
        let mut reader = tokio::io::BufReader::new(index_file);
        let mut buf: Vec<u8> = vec![0; 16];

        // if there is a not complete entry, will err
        for _ in 0..(len + 15) / 16 {
            reader.read_exact(&mut buf).await?;
            let (key, offset, size) = index_entry(&buf);

            if offset == 0 || size.is_deleted() {
                self.delete(key)
                    .await
                    .map_err(|err| NeedleError::BoxError(err.into()))?;
            } else {
                self.set(key, NeedleValue { offset, size })
                    .await
                    .map_err(|err| NeedleError::BoxError(err.into()))?;
            }
        }

        self.index_file = Some(path.to_string());
        Ok(())
    }

    pub async fn set(
        &mut self,
        key: NeedleId,
        index: NeedleValue,
    ) -> Result<Option<NeedleValue>, VolumeError> {
        debug!("needle map set key: {}, {}", key, index);
        if key > self.metric.maximum_file_key {
            self.metric.maximum_file_key = key;
        }
        self.metric.file_count += 1;
        self.metric.file_bytes += index.size.0 as u64;
        let old = self.needle_value_map.set(key, index);

        if let Some(n) = old {
            self.metric.deleted_count += 1;
            self.metric.deleted_bytes += n.size.0 as u64;
        }

        self.append_to_index_file(key, index).await?;

        Ok(old)
    }

    pub async fn delete(&mut self, key: NeedleId) -> Result<Option<NeedleValue>, VolumeError> {
        let deleted = self.needle_value_map.delete(key);

        if let Some(index) = deleted {
            self.metric.deleted_count += 1;
            self.metric.deleted_bytes += index.size.0 as u64;
            self.append_to_index_file(key, index).await?;
            debug!("needle map delete key: {} {}", key, index);
        }

        Ok(deleted)
    }

    pub fn get(&self, key: NeedleId) -> Option<NeedleValue> {
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

    pub fn max_file_key(&self) -> NeedleId {
        self.metric.maximum_file_key
    }

    pub fn content_size(&self) -> u64 {
        self.metric.file_bytes
    }

    pub async fn index_file_size(&self) -> Result<u64, VolumeError> {
        let size = match self.index_file.as_ref() {
            Some(path) => tokio::fs::metadata(path).await?.len(),
            None => 0,
        };
        Ok(size)
    }

    pub async fn append_to_index_file(
        &mut self,
        key: NeedleId,
        value: NeedleValue,
    ) -> Result<(), VolumeError> {
        if let Some(path) = self.index_file.as_ref() {
            let mut buf = vec![];
            buf.put_u64(key);
            buf.put_u32(value.offset);
            buf.put_i32(value.size.0);

            let mut file = file::append(path).await?;
            if let Err(err) = file.write_all(&buf).await {
                error!(
                    "failed to write index file, volume {}, error: {err}",
                    self.volume_id
                );
                return Err(VolumeError::Io(err));
            }
        }
        Ok(())
    }
}

pub fn index_entry(mut buf: &[u8]) -> (NeedleId, Offset, Size) {
    let key = buf.get_u64();
    let offset = buf.get_u32();
    let size = Size(buf.get_i32());

    debug!("index entry: key: {key}, offset: {offset}, size: {size}");
    (key, offset, size)
}

pub async fn walk_index_file<T>(path: &str, walk: T) -> Result<(), VolumeError>
where
    T: Fn(NeedleId, Offset, Size) -> BoxFuture<'static, Result<(), NeedleError>>,
{
    let index_file = file::open(path).await?;
    let len = index_file.metadata().await?.len();
    let mut reader = tokio::io::BufReader::new(index_file);
    let mut buf: Vec<u8> = vec![0; 16];

    // if there is a not complete entry, will err
    for _ in 0..(len + 15) / 16 {
        reader.read_exact(&mut buf).await?;

        let (key, offset, size) = index_entry(&buf);
        walk(key, offset, size).await?;
    }

    Ok(())
}
