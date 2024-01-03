use std::{
    io::{BufReader, Read},
    result::Result,
    sync::Arc,
};

use bytes::{Buf, BufMut};
use helyim_fs::File;
use tracing::{debug, error};

use crate::storage::{
    needle::{metric::Metric, MemoryNeedleValueMap, NeedleValue, NeedleValueMap},
    types::{Offset, Size},
    NeedleError, NeedleId, VolumeError, VolumeId,
};

#[derive(Copy, Clone, Debug, Default)]
pub enum NeedleMapType {
    #[default]
    NeedleMapInMemory = 0,
}

pub struct NeedleMapper {
    volume_id: VolumeId,
    needle_value_map: Box<dyn NeedleValueMap>,
    index_file: Option<File>,
    metric: Arc<Metric>,
}

impl Default for NeedleMapper {
    fn default() -> Self {
        NeedleMapper {
            volume_id: 0,
            needle_value_map: Box::new(MemoryNeedleValueMap::new()),
            metric: Arc::new(Metric::default()),
            index_file: None,
        }
    }
}

impl NeedleMapper {
    pub fn new(volume_id: VolumeId, kind: NeedleMapType) -> NeedleMapper {
        #[allow(unreachable_patterns)]
        match kind {
            NeedleMapType::NeedleMapInMemory => NeedleMapper {
                needle_value_map: Box::new(MemoryNeedleValueMap::new()),
                volume_id,
                ..Default::default()
            },
            _ => panic!("not support map type: {:?}", kind),
        }
    }

    pub async fn load_index_file(&mut self, index_file: File) -> Result<(), VolumeError> {
        let std_file = index_file.to_std()?;

        // walk index file
        let len = std_file.metadata()?.len();
        let mut reader = BufReader::new(std_file);
        let mut buf: Vec<u8> = vec![0; 16];

        // if there is a not complete entry, will err
        for _ in 0..(len + 15) / 16 {
            reader.read_exact(&mut buf)?;

            let (key, offset, size) = read_index_entry(&buf);
            if offset == 0 || size.is_deleted() {
                self.delete(key)
                    .await
                    .map_err(|err| NeedleError::Box(err.into()))?;
            } else {
                self.set(key, NeedleValue { offset, size })
                    .await
                    .map_err(|err| NeedleError::Box(err.into()))?;
            }
        }
        self.index_file = Some(index_file);
        Ok(())
    }

    pub async fn set(
        &self,
        key: NeedleId,
        index: NeedleValue,
    ) -> Result<Option<NeedleValue>, VolumeError> {
        debug!("needle map set key: {}, {}", key, index);

        self.metric.maybe_max_file_key(key);
        self.metric.add_file(index.size);

        let old = self.needle_value_map.set(key, index);
        if let Some(n) = old {
            self.metric.delete_file(n.size);
        }

        self.append_to_index_file(key, index).await?;

        Ok(old)
    }

    pub async fn delete(&self, key: NeedleId) -> Result<Option<NeedleValue>, VolumeError> {
        let deleted = self.needle_value_map.delete(key);

        if let Some(index) = deleted {
            self.metric.delete_file(index.size);
            self.append_to_index_file(key, index).await?;
            debug!("needle map delete key: {} -> {}", key, index);
        }

        Ok(deleted)
    }

    pub fn get(&self, key: NeedleId) -> Option<NeedleValue> {
        self.needle_value_map.get(key)
    }

    pub fn file_count(&self) -> u64 {
        self.metric.file_count()
    }

    pub fn deleted_count(&self) -> u64 {
        self.metric.deleted_count()
    }

    pub fn deleted_bytes(&self) -> u64 {
        self.metric.deleted_bytes()
    }

    pub fn max_file_key(&self) -> NeedleId {
        self.metric.max_file_key()
    }

    pub fn content_size(&self) -> u64 {
        self.metric.file_bytes()
    }

    pub fn index_file_size(&self) -> Result<u64, VolumeError> {
        let size = match self.index_file.as_ref() {
            Some(file) => file.metadata()?.len(),
            None => 0,
        };
        Ok(size)
    }

    pub async fn append_to_index_file(
        &self,
        key: NeedleId,
        value: NeedleValue,
    ) -> Result<(), VolumeError> {
        if let Some(file) = self.index_file.as_ref() {
            let mut buf = vec![];
            buf.put_u64(key);
            buf.put_u32(value.offset.0);
            buf.put_i32(value.size.0);

            let offset = file.metadata()?.len();
            let (write, _) = file.write_all_at(buf, offset).await;
            if let Err(err) = write {
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

pub fn read_index_entry(mut buf: &[u8]) -> (NeedleId, Offset, Size) {
    let key = buf.get_u64();
    let offset = Offset(buf.get_u32());
    let size = Size(buf.get_i32());

    debug!("index entry: key: {key}, offset: {offset}, size: {size}");
    (key, offset, size)
}

// walks through index file, call fn(key, offset, size), stop with error returned by fn
pub fn walk_index_file<T>(f: &mut std::fs::File, mut walk: T) -> Result<(), VolumeError>
where
    T: FnMut(NeedleId, Offset, Size) -> Result<(), NeedleError>,
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
