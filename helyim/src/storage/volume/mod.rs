use std::{
    fmt::Display,
    io::{ErrorKind, SeekFrom},
    path::Path,
    result::Result as StdResult,
    sync::Arc,
};

use bytes::{Buf, BufMut};
use faststr::FastStr;
use rustix::fs::ftruncate;
use tokio::{
    fs::{metadata, remove_file, File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::RwLock,
};
use tracing::{debug, error, info};

use crate::{
    storage::{
        needle::{Needle, NeedleValue, NEEDLE_PADDING_SIZE},
        needle_map::{NeedleMapType, NeedleMapper},
        ttl::Ttl,
        types::Size,
        version::{Version, CURRENT_VERSION},
        volume::checking::check_volume_data_integrity,
        NeedleError, VolumeError, VolumeId,
    },
    util::time::{get_time, now},
};

mod checking;

mod replica_placement;
pub use replica_placement::ReplicaPlacement;

#[allow(dead_code)]
pub mod vacuum;
mod volume_info;
pub use volume_info::VolumeInfo;

pub const SUPER_BLOCK_SIZE: usize = 8;

pub const DATA_FILE_SUFFIX: &str = "dat";
pub const COMPACT_DATA_FILE_SUFFIX: &str = "cpd";
pub const IDX_FILE_SUFFIX: &str = "idx";
pub const COMPACT_IDX_FILE_SUFFIX: &str = "cpx";

#[derive(Debug, Copy, Clone)]
pub struct SuperBlock {
    pub version: Version,
    pub replica_placement: ReplicaPlacement,
    pub ttl: Ttl,
    pub compact_revision: u16,
}

impl Default for SuperBlock {
    fn default() -> Self {
        SuperBlock {
            version: CURRENT_VERSION,
            replica_placement: ReplicaPlacement::default(),
            ttl: Ttl::default(),
            compact_revision: 0,
        }
    }
}

impl SuperBlock {
    pub fn parse(buf: [u8; SUPER_BLOCK_SIZE]) -> StdResult<SuperBlock, VolumeError> {
        let rp = ReplicaPlacement::from_u8(buf[1])?;
        let ttl = Ttl::from(&buf[2..4]);
        let compact_revision = (&buf[4..6]).get_u16();

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
        (&mut buf[4..6]).put_u16(self.compact_revision);
        buf
    }
}

#[derive(Default)]
pub struct Volume {
    id: VolumeId,
    dir: FastStr,
    pub collection: FastStr,
    needle_mapper: NeedleMapper,
    needle_map_type: NeedleMapType,

    data_file: Option<File>,
    data_filename: String,
    index_filename: String,

    readonly: bool,
    pub super_block: SuperBlock,
    last_modified: u64,
    last_compact_index_offset: u64,
    last_compact_revision: u16,
}

impl Display for Volume {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{{id:{}, dir:{}, collection: {}, replica_placement: {}, ttl: {}, read_only: {}}}",
            self.id,
            self.dir,
            self.collection,
            self.super_block.replica_placement,
            self.super_block.ttl,
            self.readonly
        )
    }
}

impl Volume {
    pub async fn new(
        dir: FastStr,
        collection: FastStr,
        id: VolumeId,
        needle_map_type: NeedleMapType,
        replica_placement: ReplicaPlacement,
        ttl: Ttl,
        _preallocate: i64,
    ) -> StdResult<Volume, VolumeError> {
        let sb = SuperBlock {
            replica_placement,
            ttl,
            ..Default::default()
        };

        let mut v = Volume {
            id,
            dir: dir.clone(),
            collection,
            super_block: sb,
            data_file: None,
            data_filename: String::default(),
            index_filename: String::default(),
            needle_map_type,
            needle_mapper: NeedleMapper::new(id, needle_map_type),
            readonly: false,
            last_compact_index_offset: 0,
            last_compact_revision: 0,
            last_modified: 0,
        };

        v.load(true, true).await?;
        debug!("load volume {id} success, path: {dir}");
        Ok(v)
    }

    pub async fn load(
        &mut self,
        create_if_missing: bool,
        load_index: bool,
    ) -> StdResult<(), VolumeError> {
        if self.data_file.is_some() {
            return Err(VolumeError::HasLoaded(self.id));
        }

        let name = self.data_file_name();
        debug!("loading volume: {}", name);

        let mut has_super_block = false;
        let meta = match metadata(&name).await {
            Ok(m) => {
                if m.len() >= SUPER_BLOCK_SIZE as u64 {
                    has_super_block = true;
                }
                m
            }
            Err(err) => {
                debug!("get metadata err: {err}");
                if err.kind() == ErrorKind::NotFound && create_if_missing {
                    // TODO support preallocate
                    OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .mode(0o644)
                        .open(&name)
                        .await?;
                    debug!("create volume {} data file success", self.id);
                    metadata(&name).await?
                } else {
                    return Err(VolumeError::Io(err));
                }
            }
        };

        if meta.permissions().readonly() {
            let file = OpenOptions::new().read(true).open(&name).await?;
            self.data_file = Some(file);
            self.readonly = true;
        } else {
            self.last_modified = get_time(meta.modified()?)?.as_secs();

            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .mode(0o644)
                .open(&name)
                .await?;
            self.data_file = Some(file);
        }
        self.data_filename = name;

        if has_super_block {
            self.read_super_block().await?;
        } else {
            self.write_super_block().await?;
        }

        if load_index {
            self.index_filename = self.index_file_name();
            let mut index_file = OpenOptions::new()
                .read(true)
                .create(true)
                .write(true)
                .open(self.index_filename())
                .await?;

            if let Err(err) = check_volume_data_integrity(self, &mut index_file).await {
                self.readonly = true;
                error!(
                    "volume data integrity checking failed. volume: {}, filename: {}, {err}",
                    self.id,
                    self.data_filename()
                );
            }
            self.needle_mapper = NeedleMapper::new(self.id, self.needle_map_type);
            self.needle_mapper.load_idx_file(index_file).await?;
            info!("load index file `{}` success", self.index_filename());
        }

        Ok(())
    }

    pub async fn write_needle(&mut self, mut needle: Needle) -> StdResult<Needle, VolumeError> {
        let volume_id = self.id;
        if self.readonly {
            return Err(VolumeError::Readonly(volume_id));
        }

        let version = self.version();

        let mut file = self.data_file()?;
        let mut offset = file.seek(SeekFrom::End(0)).await?;
        if offset % NEEDLE_PADDING_SIZE as u64 != 0 {
            offset = offset + (NEEDLE_PADDING_SIZE as u64 - offset % NEEDLE_PADDING_SIZE as u64);
            offset = file.seek(SeekFrom::Start(offset)).await?;
        }

        if let Err(err) = needle.append(&mut file, version).await {
            error!(
                "volume {volume_id}: write needle {} error: {err}, will do ftruncate.",
                needle.id
            );
            ftruncate(file, offset)?;
            return Err(err.into());
        }

        offset /= NEEDLE_PADDING_SIZE as u64;
        let nv = NeedleValue {
            offset: offset as u32,
            size: needle.size,
        };
        self.needle_mapper.set(needle.id, nv).await?;

        if self.last_modified < needle.last_modified {
            self.last_modified = needle.last_modified;
        }

        Ok(needle)
    }

    pub async fn delete_needle(&mut self, mut needle: Needle) -> StdResult<Size, VolumeError> {
        if self.readonly {
            return Err(VolumeError::Readonly(self.id));
        }

        let mut nv = match self.needle_mapper.get(needle.id) {
            Some(nv) => nv,
            None => return Ok(Size(0)),
        };
        nv.offset = 0;

        self.needle_mapper.set(needle.id, nv).await?;
        needle.set_is_delete();
        needle.data.clear();

        let version = self.version();

        let mut file = self.data_file()?;
        needle.append(&mut file, version).await?;
        Ok(nv.size)
    }

    pub async fn read_needle(&mut self, mut needle: Needle) -> StdResult<Needle, VolumeError> {
        match self.needle_mapper.get(needle.id) {
            Some(nv) => {
                if nv.offset == 0 || nv.size.is_deleted() {
                    return Err(NeedleError::Deleted(self.id, needle.id).into());
                }

                let version = self.version();
                needle
                    .read_data(self.data_file()?, nv.offset, nv.size, version)
                    .await?;

                if needle.has_ttl() && needle.has_last_modified_date() {
                    let minutes = needle.ttl.minutes();
                    if minutes > 0
                        && now().as_secs() >= (needle.last_modified + minutes as u64 * 60)
                    {
                        return Err(NeedleError::Expired(self.id, needle.id).into());
                    }
                }
                Ok(needle)
            }
            None => Err(NeedleError::NotFound(self.id, needle.id).into()),
        }
    }

    pub fn version(&self) -> Version {
        self.super_block.version
    }

    pub fn filename(&self) -> String {
        let mut dirname = self.dir.to_string();
        if !dirname.ends_with('/') {
            dirname.push('/');
        }
        if self.collection.is_empty() {
            format!("{}{}", dirname, self.id)
        } else {
            format!("{}{}_{}", dirname, self.collection, self.id)
        }
    }

    pub fn get_volume_info(&self) -> VolumeInfo {
        VolumeInfo {
            id: self.id,
            size: self.content_size(),
            replica_placement: self.super_block.replica_placement,
            ttl: self.super_block.ttl,
            collection: self.collection.clone(),
            version: self.version(),
            file_count: self.needle_mapper.file_count() as i64,
            delete_count: self.needle_mapper.deleted_count() as i64,
            delete_bytes: self.needle_mapper.deleted_bytes(),
            read_only: self.readonly,
        }
    }

    pub fn is_readonly(&self) -> bool {
        self.readonly
    }

    pub async fn destroy(&self) -> StdResult<(), VolumeError> {
        if self.readonly {
            return Err(VolumeError::Readonly(self.id));
        }

        remove_file(Path::new(&self.data_filename())).await?;
        remove_file(Path::new(&self.index_filename())).await?;

        Ok(())
    }

    pub fn deleted_bytes(&self) -> u64 {
        self.needle_mapper.deleted_bytes()
    }

    pub fn deleted_count(&self) -> u64 {
        self.needle_mapper.deleted_count()
    }

    pub fn max_file_key(&self) -> u64 {
        self.needle_mapper.max_file_key()
    }

    pub fn file_count(&self) -> u64 {
        self.needle_mapper.file_count()
    }

    pub async fn size(&self) -> StdResult<u64, VolumeError> {
        Ok(metadata(self.data_filename()).await?.len())
    }

    pub fn data_file(&mut self) -> StdResult<&mut File, VolumeError> {
        match self.data_file.as_mut() {
            Some(file) => Ok(file),
            None => Err(VolumeError::NotLoad(self.id)),
        }
    }

    pub fn data_filename(&self) -> &str {
        &self.data_filename
    }

    pub fn index_filename(&self) -> &str {
        &self.index_filename
    }

    pub fn content_size(&self) -> u64 {
        self.needle_mapper.content_size()
    }

    /// volume is expired if modified time + volume ttl < now
    /// except when volume is empty
    /// or when the volume does not have a ttl
    /// or when volumeSizeLimit is 0 when server just starts
    pub fn expired(&self, volume_size_limit: u64) -> bool {
        if volume_size_limit == 0 {
            return false;
        }

        if self.content_size() == 0 {
            return false;
        }

        // change self ttl to option?
        if self.super_block.ttl.minutes() == 0 {
            return false;
        }

        if now().as_secs() > self.last_modified + self.super_block.ttl.minutes() as u64 * 60 {
            return true;
        }

        false
    }

    pub fn need_to_replicate(&self) -> bool {
        self.super_block.replica_placement.copy_count() > 1
    }

    /// wait either maxDelayMinutes or 10% of ttl minutes
    pub fn expired_long_enough(&self, max_delay_minutes: u64) -> bool {
        let ttl = self.super_block.ttl;
        if ttl.minutes() == 0 {
            return false;
        }

        let mut delay: u64 = ttl.minutes() as u64 / 10;
        if delay > max_delay_minutes {
            delay = max_delay_minutes;
        }

        if (ttl.minutes() as u64 + delay) * 60 + self.last_modified < now().as_secs() {
            return true;
        }

        false
    }
}

impl Volume {
    fn data_file_name(&self) -> String {
        format!("{}.{DATA_FILE_SUFFIX}", self.filename())
    }

    fn index_file_name(&self) -> String {
        format!("{}.{IDX_FILE_SUFFIX}", self.filename())
    }
}

impl Volume {
    async fn write_super_block(&mut self) -> StdResult<(), VolumeError> {
        let bytes = self.super_block.as_bytes();

        let file = self.data_file()?;
        if file.metadata().await?.len() != 0 {
            return Ok(());
        }
        file.seek(SeekFrom::Start(0)).await?;
        file.write_all(&bytes).await?;
        debug!("write super block success");
        Ok(())
    }

    async fn read_super_block(&mut self) -> StdResult<(), VolumeError> {
        let mut buf = [0; SUPER_BLOCK_SIZE];
        {
            let file = self.data_file()?;
            file.seek(SeekFrom::Start(0)).await?;
            file.read_exact(&mut buf).await?;
        }
        self.super_block = SuperBlock::parse(buf)?;

        Ok(())
    }
}

async fn load_volume_without_index(
    dirname: FastStr,
    collection: FastStr,
    id: VolumeId,
    needle_map_type: NeedleMapType,
) -> StdResult<Volume, VolumeError> {
    let mut volume = Volume {
        dir: dirname,
        collection,
        id,
        needle_map_type,
        ..Default::default()
    };
    volume.load(false, false).await?;
    Ok(volume)
}

#[derive(Clone)]
pub struct VolumeRef(Arc<RwLock<Volume>>);

impl VolumeRef {
    pub async fn new(
        dir: FastStr,
        collection: FastStr,
        id: VolumeId,
        needle_map_type: NeedleMapType,
        replica_placement: ReplicaPlacement,
        ttl: Ttl,
        preallocate: i64,
    ) -> StdResult<VolumeRef, VolumeError> {
        Ok(Self(Arc::new(RwLock::new(
            Volume::new(
                dir,
                collection,
                id,
                needle_map_type,
                replica_placement,
                ttl,
                preallocate,
            )
            .await?,
        ))))
    }

    pub async fn read(&self) -> tokio::sync::RwLockReadGuard<'_, Volume> {
        self.0.read().await
    }

    pub async fn write(&self) -> tokio::sync::RwLockWriteGuard<'_, Volume> {
        self.0.write().await
    }
}

#[cfg(test)]
pub mod tests {

    use bytes::Bytes;
    use faststr::FastStr;
    use rand::random;

    use crate::storage::{
        crc, volume::Volume, FileId, Needle, NeedleMapType, ReplicaPlacement, Ttl,
    };

    pub async fn setup(dir: FastStr) -> Volume {
        let mut volume = Volume::new(
            dir,
            FastStr::empty(),
            1,
            NeedleMapType::NeedleMapInMemory,
            ReplicaPlacement::default(),
            Ttl::default(),
            0,
        )
        .await
        .unwrap();

        for i in 0..1000 {
            let fid = FileId::new(volume.id, i, random::<u32>());
            let data = Bytes::from_static(b"Hello World");
            let checksum = crc::checksum(&data);
            let mut needle = Needle {
                data,
                checksum,
                ..Default::default()
            };
            needle
                .parse_path(&format!("{:x}{:08x}", fid.key, fid.hash))
                .unwrap();
            volume.write_needle(needle).await.unwrap();
        }

        volume
    }
}
