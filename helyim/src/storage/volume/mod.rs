use std::{
    fmt::Display,
    fs::{self, metadata, File},
    io::ErrorKind,
    os::unix::fs::{FileExt, OpenOptionsExt},
    path::Path,
    result::Result as StdResult,
    sync::Arc,
};

use bytes::{Buf, BufMut};
use faststr::FastStr;
use rustix::fs::ftruncate;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

use crate::{
    storage::{
        needle::{
            read_needle_header, Needle, NeedleValue, NEEDLE_HEADER_SIZE, NEEDLE_PADDING_SIZE,
        },
        needle_map::{NeedleMapType, NeedleMapper},
        ttl::Ttl,
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

use crate::storage::types::Offset;

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
    data_file: Option<File>,
    needle_mapper: NeedleMapper,
    needle_map_type: NeedleMapType,
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
    pub fn new(
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
            needle_map_type,
            needle_mapper: NeedleMapper::new(id, needle_map_type),
            readonly: false,
            last_compact_index_offset: 0,
            last_compact_revision: 0,
            last_modified: 0,
        };

        v.load(true, true)?;
        debug!("load volume {id} success, path: {dir}");
        Ok(v)
    }

    pub fn load(
        &mut self,
        create_if_missing: bool,
        load_index: bool,
    ) -> StdResult<(), VolumeError> {
        if self.data_file.is_some() {
            return Err(VolumeError::HasLoaded(self.id));
        }

        let name = self.data_filename();
        let mut has_super_block = false;
        let meta = match metadata(&name) {
            Ok(m) => {
                if m.len() >= SUPER_BLOCK_SIZE as u64 {
                    has_super_block = true;
                }
                m
            }
            Err(err) => {
                debug!("get data file metadata error:{err}, name: {name}");
                if err.kind() == ErrorKind::NotFound && create_if_missing {
                    // TODO support preallocate
                    fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .mode(0o644)
                        .open(&name)?;
                    debug!("create volume {} data file success", self.id);
                    metadata(&name)?
                } else {
                    return Err(VolumeError::Io(err));
                }
            }
        };

        if meta.permissions().readonly() {
            let file = fs::OpenOptions::new().read(true).open(&name)?;
            self.data_file = Some(file);
            self.readonly = true;
        } else {
            let file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .mode(0o644)
                .open(&name)?;

            self.last_modified = get_time(meta.modified()?)?.as_secs();
            self.data_file = Some(file);
        }

        if has_super_block {
            self.read_super_block()?;
        } else {
            self.write_super_block()?;
        }

        if load_index {
            let index_file = fs::OpenOptions::new()
                .read(true)
                .create(true)
                .write(true)
                .open(self.index_filename())?;

            if let Err(err) = check_volume_data_integrity(self, &index_file) {
                self.readonly = true;
                error!(
                    "volume data integrity checking failed. volume: {}, filename: {}, {err}",
                    self.id,
                    self.data_filename()
                );
            }
            self.needle_mapper = NeedleMapper::new(self.id, self.needle_map_type);
            self.needle_mapper.load_idx_file(index_file)?;
            info!("load index file `{}` success", self.index_filename());
        }

        Ok(())
    }

    pub fn write_needle(&mut self, needle: &mut Needle) -> StdResult<u32, VolumeError> {
        let volume_id = self.id;
        if self.readonly {
            return Err(VolumeError::Readonly(volume_id));
        }

        let version = self.version();
        let file = self.data_file()?;

        let mut offset = file.metadata()?.len();
        if offset % NEEDLE_PADDING_SIZE as u64 != 0 {
            offset = offset + (NEEDLE_PADDING_SIZE as u64 - offset % NEEDLE_PADDING_SIZE as u64);
        }

        if let Err(err) = needle.append(file, offset, version) {
            error!(
                "volume {volume_id}: write needle {} error: {err}, will do ftruncate.",
                needle.id
            );
            ftruncate(file, offset)?;
            return Err(err.into());
        }

        let nv = NeedleValue {
            offset: offset.into(),
            size: needle.size,
        };
        self.needle_mapper.set(needle.id, nv)?;

        if self.last_modified < needle.last_modified {
            self.last_modified = needle.last_modified;
        }

        Ok(needle.data_size())
    }

    pub fn delete_needle(&mut self, needle: &mut Needle) -> StdResult<u32, VolumeError> {
        if self.readonly {
            return Err(VolumeError::Readonly(self.id));
        }

        let mut nv = match self.needle_mapper.get(needle.id) {
            Some(nv) => nv,
            None => return Ok(0),
        };
        nv.offset = Offset(0);

        self.needle_mapper.set(needle.id, nv)?;
        // TODO: can be removed
        needle.set_is_delete();

        let data_size = needle.data_size();
        needle.data.clear();

        let version = self.version();
        let file = self.data_file()?;

        let mut offset = file.metadata()?.len();
        if offset % NEEDLE_PADDING_SIZE as u64 != 0 {
            offset = offset + (NEEDLE_PADDING_SIZE as u64 - offset % NEEDLE_PADDING_SIZE as u64);
        }
        needle.append(file, offset, version)?;
        Ok(data_size)
    }

    pub fn read_needle(&self, needle: &mut Needle) -> StdResult<u32, VolumeError> {
        match self.needle_mapper.get(needle.id) {
            Some(nv) => {
                if nv.offset == 0 || nv.size.is_deleted() {
                    return Err(NeedleError::Deleted(self.id, needle.id).into());
                }

                let version = self.version();
                let data_file = self.data_file()?;
                needle.read_data(data_file, nv.offset, nv.size, version)?;
                let data_size = needle.data_size();
                if !needle.has_ttl() {
                    return Ok(data_size);
                }
                let minutes = needle.ttl.minutes();
                if minutes == 0 {
                    return Ok(data_size);
                }
                if !needle.has_last_modified_date() {
                    return Ok(data_size);
                }
                if now().as_secs() < (needle.last_modified + minutes as u64 * 60) {
                    return Ok(data_size);
                }
                Err(NeedleError::Expired(self.id, needle.id).into())
            }
            None => Err(NeedleError::NotFound(needle.id).into()),
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

    pub fn destroy(&self) -> StdResult<(), VolumeError> {
        if self.readonly {
            return Err(VolumeError::Readonly(self.id));
        }

        fs::remove_file(Path::new(&self.data_filename()))?;
        fs::remove_file(Path::new(&self.index_filename()))?;

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

    pub fn data_file_size(&self) -> StdResult<u64, VolumeError> {
        let file = self.data_file()?;
        Ok(file.metadata()?.len())
    }

    pub fn data_file(&self) -> StdResult<&File, VolumeError> {
        match self.data_file.as_ref() {
            Some(data_file) => Ok(data_file),
            None => Err(VolumeError::NotLoad(self.id)),
        }
    }

    pub fn data_filename(&self) -> String {
        format!("{}.{DATA_FILE_SUFFIX}", self.filename())
    }

    pub fn index_filename(&self) -> String {
        format!("{}.{IDX_FILE_SUFFIX}", self.filename())
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
    fn write_super_block(&self) -> StdResult<(), VolumeError> {
        let bytes = self.super_block.as_bytes();
        let file = self.data_file()?;
        if file.metadata()?.len() != 0 {
            return Ok(());
        }

        file.write_all_at(&bytes, 0)?;
        debug!("write super block success");
        Ok(())
    }

    fn read_super_block(&mut self) -> StdResult<(), VolumeError> {
        let mut buf = [0; SUPER_BLOCK_SIZE];
        {
            let file = self.data_file()?;
            file.read_exact_at(&mut buf, 0)?;
        }
        self.super_block = SuperBlock::parse(buf)?;

        Ok(())
    }
}

fn load_volume_without_index(
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
    volume.load(false, false)?;
    Ok(volume)
}

pub fn scan_volume_file<VSB, VN>(
    dirname: FastStr,
    collection: FastStr,
    id: VolumeId,
    needle_map_type: NeedleMapType,
    read_needle_body: bool,
    mut visit_super_block: VSB,
    mut visit_needle: VN,
) -> StdResult<(), VolumeError>
where
    VSB: FnMut(&mut SuperBlock) -> StdResult<(), VolumeError>,
    VN: FnMut(&mut Needle, u64) -> StdResult<(), VolumeError>,
{
    let mut volume = load_volume_without_index(dirname, collection, id, needle_map_type)?;
    visit_super_block(&mut volume.super_block)?;

    let version = volume.version();
    let mut offset = SUPER_BLOCK_SIZE as u64;

    let (mut needle, mut rest) = read_needle_header(volume.data_file()?, version, offset)?;

    loop {
        if read_needle_body {
            if let Err(err) = needle.read_needle_body(
                volume.data_file()?,
                offset + NEEDLE_HEADER_SIZE as u64,
                rest,
                version,
            ) {
                error!("cannot read needle body when scanning volume file, {err}");
            }
        }

        visit_needle(&mut needle, offset)?;
        offset += (NEEDLE_HEADER_SIZE + rest) as u64;

        match read_needle_header(volume.data_file()?, version, offset) {
            Ok((n, body_len)) => {
                needle = n;
                rest = body_len;
            }
            Err(NeedleError::Io(err)) => {
                if err.kind() == ErrorKind::UnexpectedEof {
                    return Ok(());
                }
            }
            Err(err) => {
                return Err(VolumeError::Needle(err));
            }
        }
    }
}

#[derive(Clone)]
pub struct VolumeRef(Arc<RwLock<Volume>>);

impl VolumeRef {
    pub fn new(
        dir: FastStr,
        collection: FastStr,
        id: VolumeId,
        needle_map_type: NeedleMapType,
        replica_placement: ReplicaPlacement,
        ttl: Ttl,
        preallocate: i64,
    ) -> StdResult<VolumeRef, VolumeError> {
        Ok(Self(Arc::new(RwLock::new(Volume::new(
            dir,
            collection,
            id,
            needle_map_type,
            replica_placement,
            ttl,
            preallocate,
        )?))))
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
    use tempfile::Builder;

    use crate::storage::{
        crc,
        volume::{scan_volume_file, SuperBlock, Volume},
        FileId, Needle, NeedleMapType, ReplicaPlacement, Ttl, VolumeError,
    };

    pub fn setup(dir: FastStr) -> Volume {
        let mut volume = Volume::new(
            dir,
            FastStr::empty(),
            1,
            NeedleMapType::NeedleMapInMemory,
            ReplicaPlacement::default(),
            Ttl::default(),
            0,
        )
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
            volume.write_needle(&mut needle).unwrap();
        }

        volume
    }

    #[test]
    pub fn test_scan_volume_file() {
        let dir = Builder::new()
            .prefix("scan_volume_file")
            .tempdir_in(".")
            .unwrap();
        let dir = FastStr::new(dir.path().to_str().unwrap());
        let volume = setup(dir.clone());

        scan_volume_file(
            dir.clone(),
            FastStr::empty(),
            volume.id,
            volume.needle_map_type,
            true,
            |super_block: &mut SuperBlock| -> Result<(), VolumeError> { Ok(()) },
            |needle, offset| -> Result<(), VolumeError> {
                assert_eq!(needle.data_size, 11);
                Ok(())
            },
        )
        .unwrap();
    }
}
