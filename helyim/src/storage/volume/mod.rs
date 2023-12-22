use std::{
    fmt::Display,
    fs::{self, metadata, File},
    io::ErrorKind,
    os::unix::fs::{FileExt, OpenOptionsExt},
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering},
        Arc,
    },
    time::SystemTimeError,
};

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use bytes::{Buf, BufMut};
use faststr::FastStr;
use parking_lot::Mutex;
use rustix::fs::ftruncate;
use serde_json::json;
use tracing::{debug, error, info};

use crate::{
    storage::{
        needle::{
            read_needle_header, Needle, NeedleMapType, NeedleMapper, NeedleValue,
            NEEDLE_HEADER_SIZE, NEEDLE_PADDING_SIZE,
        },
        ttl::Ttl,
        version::{Version, CURRENT_VERSION},
        volume::checking::check_volume_data_integrity,
        VolumeId,
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

use crate::storage::{needle::NeedleError, types::Offset};

pub const SUPER_BLOCK_SIZE: usize = 8;

pub const DATA_FILE_SUFFIX: &str = "dat";
pub const COMPACT_DATA_FILE_SUFFIX: &str = "cpd";
pub const IDX_FILE_SUFFIX: &str = "idx";
pub const COMPACT_IDX_FILE_SUFFIX: &str = "cpx";

#[derive(Debug)]
pub struct SuperBlock {
    pub version: Version,
    pub replica_placement: ReplicaPlacement,
    pub ttl: Ttl,
    compact_revision: AtomicU16,
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
    pub fn parse(buf: [u8; SUPER_BLOCK_SIZE]) -> Result<SuperBlock, VolumeError> {
        let rp = ReplicaPlacement::from_u8(buf[1])?;
        let ttl = Ttl::from(&buf[2..4]);
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

#[derive(Default)]
pub struct Volume {
    id: VolumeId,
    dir: FastStr,
    pub collection: FastStr,

    data_file: Option<File>,
    data_file_mutex: Mutex<()>,

    needle_mapper: Option<Arc<NeedleMapper>>,
    needle_map_type: NeedleMapType,
    pub super_block: Arc<SuperBlock>,

    readonly: Arc<AtomicBool>,
    last_modified: Arc<AtomicU64>,
    last_compact_index_offset: Arc<AtomicU64>,
    last_compact_revision: Arc<AtomicU16>,
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
            self.readonly()
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
    ) -> Result<Volume, VolumeError> {
        let sb = SuperBlock {
            replica_placement,
            ttl,
            ..Default::default()
        };

        let mut v = Volume {
            id,
            dir: dir.clone(),
            collection,
            super_block: Arc::new(sb),
            data_file: None,
            data_file_mutex: Mutex::new(()),
            needle_map_type,
            needle_mapper: None,
            readonly: Arc::new(AtomicBool::new(true)),
            last_compact_index_offset: Arc::new(AtomicU64::new(0)),
            last_compact_revision: Arc::new(AtomicU16::new(0)),
            last_modified: Arc::new(AtomicU64::new(0)),
        };

        v.load(true, true)?;
        debug!("load volume {id} success, path: {dir}");
        Ok(v)
    }

    pub fn load(&mut self, create_if_missing: bool, load_index: bool) -> Result<(), VolumeError> {
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

        let file = if meta.permissions().readonly() {
            fs::OpenOptions::new().read(true).open(&name)?
        } else {
            self.set_last_modified(get_time(meta.modified()?)?.as_secs());
            self.set_readonly(false);
            fs::OpenOptions::new()
                .read(true)
                .write(true)
                .mode(0o644)
                .open(&name)?
        };
        self.data_file = Some(file);

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
                self.set_readonly(true);
                error!(
                    "volume data integrity checking failed. volume: {}, filename: {}, {err}",
                    self.id,
                    self.data_filename()
                );
            }
            let mut needle_mapper = NeedleMapper::new(self.id, self.needle_map_type);
            needle_mapper.load_index_file(index_file)?;
            self.needle_mapper = Some(Arc::new(needle_mapper));
            info!("load index file `{}` success", self.index_filename());
        }

        Ok(())
    }

    pub fn write_needle(&self, needle: &mut Needle) -> Result<u32, VolumeError> {
        let volume_id = self.id;
        if self.readonly() {
            return Err(VolumeError::Readonly(volume_id));
        }

        let version = self.version();
        let file = self.data_file()?;

        let mut offset = file.metadata()?.len();
        if offset % NEEDLE_PADDING_SIZE as u64 != 0 {
            offset = offset + (NEEDLE_PADDING_SIZE as u64 - offset % NEEDLE_PADDING_SIZE as u64);
        }

        {
            let _lock = self.data_file_mutex.lock();
            if let Err(err) = needle.append(file, offset, version) {
                error!(
                    "volume {volume_id}: write needle {} error: {err}, will do ftruncate.",
                    needle.id
                );
                ftruncate(file, offset)?;
                return Err(err.into());
            }
        }

        let nv = NeedleValue {
            offset: offset.into(),
            size: needle.size,
        };
        self.needle_mapper()?.set(needle.id, nv)?;

        if self.last_modified() < needle.last_modified {
            self.set_last_modified(needle.last_modified);
        }

        Ok(needle.data_size())
    }

    pub fn delete_needle(&self, needle: &mut Needle) -> Result<u32, VolumeError> {
        if self.readonly() {
            return Err(VolumeError::Readonly(self.id));
        }

        let mut nv = match self.needle_mapper()?.get(needle.id) {
            Some(nv) => nv,
            None => return Ok(0),
        };
        nv.offset = Offset(0);

        self.needle_mapper()?.set(needle.id, nv)?;
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
        {
            let _lock = self.data_file_mutex.lock();
            needle.append(file, offset, version)?;
        }
        Ok(data_size)
    }

    pub fn read_needle(&self, needle: &mut Needle) -> Result<u32, VolumeError> {
        match self.needle_mapper()?.get(needle.id) {
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
                error!("needle {} is expired, volume: {}", needle.id, self.id);
                Err(NeedleError::Expired(self.id, needle.id).into())
            }
            None => {
                error!("needle {} is not found, volume: {}", needle.id, self.id);
                Err(NeedleError::NotFound(needle.id).into())
            }
        }
    }

    pub fn needle_mapper(&self) -> Result<&NeedleMapper, VolumeError> {
        match self.needle_mapper.as_ref() {
            Some(nm) => Ok(nm),
            None => {
                error!("needle mapper is not load, volume: {}", self.id);
                Err(VolumeError::NeedleMapperNotLoad(self.id))
            }
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

    pub fn get_volume_info(&self) -> Result<VolumeInfo, VolumeError> {
        Ok(VolumeInfo {
            id: self.id,
            size: self.content_size()?,
            replica_placement: self.super_block.replica_placement,
            ttl: self.super_block.ttl,
            collection: self.collection.clone(),
            version: self.version(),
            file_count: self.file_count()? as i64,
            delete_count: self.deleted_count()? as i64,
            delete_bytes: self.deleted_bytes()?,
            read_only: self.readonly(),
        })
    }

    pub fn destroy(&self) -> Result<(), VolumeError> {
        if self.readonly() {
            return Err(VolumeError::Readonly(self.id));
        }

        fs::remove_file(Path::new(&self.data_filename()))?;
        fs::remove_file(Path::new(&self.index_filename()))?;

        Ok(())
    }

    pub fn deleted_bytes(&self) -> Result<u64, VolumeError> {
        Ok(self.needle_mapper()?.deleted_bytes())
    }

    pub fn deleted_count(&self) -> Result<u64, VolumeError> {
        Ok(self.needle_mapper()?.deleted_count())
    }

    pub fn max_file_key(&self) -> Result<u64, VolumeError> {
        Ok(self.needle_mapper()?.max_file_key())
    }

    pub fn file_count(&self) -> Result<u64, VolumeError> {
        Ok(self.needle_mapper()?.file_count())
    }

    pub fn data_file_size(&self) -> Result<u64, VolumeError> {
        let file = self.data_file()?;
        Ok(file.metadata()?.len())
    }

    pub fn data_file(&self) -> Result<&File, VolumeError> {
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

    pub fn content_size(&self) -> Result<u64, VolumeError> {
        Ok(self.needle_mapper()?.content_size())
    }

    /// volume is expired if modified time + volume ttl < now
    /// except when volume is empty
    /// or when the volume does not have a ttl
    /// or when volumeSizeLimit is 0 when server just starts
    pub fn expired(&self, volume_size_limit: u64) -> Result<bool, VolumeError> {
        if volume_size_limit == 0 {
            return Ok(false);
        }

        if self.content_size()? == 0 {
            return Ok(false);
        }

        // change self ttl to option?
        if self.super_block.ttl.minutes() == 0 {
            return Ok(false);
        }

        if now().as_secs() > self.last_modified() + self.super_block.ttl.minutes() as u64 * 60 {
            return Ok(true);
        }

        Ok(false)
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

        if (ttl.minutes() as u64 + delay) * 60 + self.last_modified() < now().as_secs() {
            return true;
        }

        false
    }
}

impl Volume {
    fn write_super_block(&self) -> Result<(), VolumeError> {
        let bytes = self.super_block.as_bytes();
        let file = self.data_file()?;
        if file.metadata()?.len() != 0 {
            return Ok(());
        }

        file.write_all_at(&bytes, 0)?;
        debug!("write super block success");
        Ok(())
    }

    fn read_super_block(&mut self) -> Result<(), VolumeError> {
        let mut buf = [0; SUPER_BLOCK_SIZE];
        {
            let file = self.data_file()?;
            file.read_exact_at(&mut buf, 0)?;
        }
        self.super_block = Arc::new(SuperBlock::parse(buf)?);
        Ok(())
    }
}

impl Volume {
    pub fn set_readonly(&self, readonly: bool) {
        self.readonly.store(readonly, Ordering::Relaxed)
    }

    pub fn readonly(&self) -> bool {
        self.readonly.load(Ordering::Relaxed)
    }

    pub fn set_last_modified(&self, last_modified: u64) {
        self.last_modified.store(last_modified, Ordering::Relaxed)
    }

    pub fn last_modified(&self) -> u64 {
        self.last_modified.load(Ordering::Relaxed)
    }

    pub fn set_last_compact_index_offset(&self, last_compact_index_offset: u64) {
        self.last_compact_index_offset
            .store(last_compact_index_offset, Ordering::Relaxed)
    }

    pub fn last_compact_index_offset(&self) -> u64 {
        self.last_compact_index_offset.load(Ordering::Relaxed)
    }

    pub fn set_last_compact_revision(&self, last_compact_revision: u16) {
        self.last_compact_revision
            .store(last_compact_revision, Ordering::Relaxed)
    }

    pub fn last_compact_revision(&self) -> u16 {
        self.last_compact_revision.load(Ordering::Relaxed)
    }
}

impl Volume {
    pub fn set_super_block(&self, super_block: SuperBlock) {
        unsafe {
            let this = self as *const Self as *mut Self;
            (*this).super_block = Arc::new(super_block);
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum VolumeError {
    #[error("Io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("error: {0}")]
    Box(#[from] Box<dyn std::error::Error + Sync + Send>),
    #[error("System time error: {0}")]
    SystemTimeError(#[from] SystemTimeError),
    #[error("Raw volume error: {0}")]
    String(String),
    #[error("Parse integer error: {0}")]
    ParseInt(#[from] std::num::ParseIntError),

    #[error("Errno: {0}")]
    Errno(#[from] rustix::io::Errno),
    #[error("Serde json error: {0}")]
    SerdeJson(#[from] serde_json::Error),
    #[error("Tonic status: {0}")]
    TonicStatus(#[from] tonic::Status),
    #[error("JoinHandle error: {0}")]
    TaskJoin(#[from] tokio::task::JoinError),
    #[error("Reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("Invalid replica placement: {0}")]
    ReplicaPlacement(String),
    #[error("No writable volumes.")]
    NoWritableVolumes,
    #[error("Volume {0} is not found.")]
    NotFound(VolumeId),
    #[error("Data integrity error: {0}")]
    DataIntegrity(String),
    #[error("Volume {0} is not loaded.")]
    NotLoad(VolumeId),
    #[error("Volume {0} has loaded.")]
    HasLoaded(VolumeId),
    #[error("Volume {0} is readonly.")]
    Readonly(VolumeId),
    #[error("Needle error: {0}")]
    Needle(#[from] NeedleError),
    #[error("NeedleMapper is not load, volume: {0}")]
    NeedleMapperNotLoad(VolumeId),
    #[error("No free space: {0}")]
    NoFreeSpace(String),

    // heartbeat
    #[error("Start heartbeat failed.")]
    StartHeartbeat,
    #[error("Send heartbeat to {0} failed.")]
    SendHeartbeat(FastStr),
    #[error("Leader changed, current leader is {0}, old leader is {1}")]
    LeaderChanged(FastStr, FastStr),
}

impl From<VolumeError> for tonic::Status {
    fn from(value: VolumeError) -> Self {
        tonic::Status::internal(value.to_string())
    }
}

impl From<nom::Err<nom::error::Error<&str>>> for VolumeError {
    fn from(value: nom::Err<nom::error::Error<&str>>) -> Self {
        Self::String(value.to_string())
    }
}

impl IntoResponse for VolumeError {
    fn into_response(self) -> Response {
        let error = self.to_string();
        let error = json!({
            "error": error
        });
        let response = (StatusCode::BAD_REQUEST, Json(error));
        response.into_response()
    }
}

fn load_volume_without_index(
    dirname: FastStr,
    collection: FastStr,
    id: VolumeId,
    needle_map_type: NeedleMapType,
) -> Result<Volume, VolumeError> {
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
) -> Result<(), VolumeError>
where
    VSB: FnMut(&Arc<SuperBlock>) -> Result<(), VolumeError>,
    VN: FnMut(&mut Needle, u64) -> Result<(), VolumeError>,
{
    let volume = load_volume_without_index(dirname, collection, id, needle_map_type)?;
    visit_super_block(&volume.super_block)?;

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

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use faststr::FastStr;
    use rand::random;
    use tempfile::Builder;

    use crate::storage::{
        crc,
        needle::NeedleMapType,
        volume::{scan_volume_file, SuperBlock, Volume, VolumeError},
        FileId, Needle, ReplicaPlacement, Ttl,
    };

    pub fn setup(dir: FastStr) -> Volume {
        let volume = Volume::new(
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
            |super_block: &Arc<SuperBlock>| -> Result<(), VolumeError> { Ok(()) },
            |needle, offset| -> Result<(), VolumeError> {
                assert_eq!(needle.data_size, 11);
                Ok(())
            },
        )
        .unwrap();
    }
}
