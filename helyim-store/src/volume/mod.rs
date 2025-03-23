use std::{
    fmt::Display,
    fs::{self, File, metadata},
    io,
    io::ErrorKind,
    os::unix::fs::{FileExt, OpenOptionsExt},
    path::Path,
    string::FromUtf8Error,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering},
    },
};

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use faststr::FastStr;
use helyim_common::{
    consts::{
        DATA_FILE_SUFFIX, IDX_FILE_SUFFIX, NEEDLE_ENTRY_SIZE, NEEDLE_PADDING_SIZE, SUPER_BLOCK_SIZE,
    },
    http::HttpError,
    parser::ParseError,
    sequence::SequenceError,
    time::{TimeError, get_time, now},
    ttl::Ttl,
    types::{NeedleId, NeedleValue, Offset, ReplicaPlacement, SuperBlock, VolumeId},
    version::Version,
};
use helyim_ec::EcVolumeError;
use helyim_topology::{TopologyError, volume::VolumeInfo};
use parking_lot::RwLock;
use rustix::fs::ftruncate;
use serde_json::json;
use tonic::Status;
use tracing::{debug, error, info};

use crate::{
    needle::{Needle, NeedleError, NeedleMapType, NeedleMapper, read_needle_header},
    volume::checking::check_volume_data_integrity,
};

pub mod checking;
pub mod delta_volume;
pub mod vacuum;

#[derive(Default)]
pub struct Volume {
    id: VolumeId,
    dir: FastStr,
    pub collection: FastStr,

    data_file: Option<File>,
    data_file_lock: RwLock<()>,

    needle_mapper: Option<NeedleMapper>,
    needle_map_type: NeedleMapType,
    pub super_block: Arc<SuperBlock>,

    no_write_or_delete: Arc<AtomicBool>,
    no_write_can_delete: Arc<AtomicBool>,
    is_compacting: Arc<AtomicBool>,

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
        _preallocate: u64,
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
            data_file_lock: RwLock::new(()),
            needle_map_type,
            needle_mapper: None,
            no_write_or_delete: Arc::new(AtomicBool::new(false)),
            no_write_can_delete: Arc::new(AtomicBool::new(false)),
            is_compacting: Arc::new(AtomicBool::new(false)),

            last_compact_index_offset: Arc::new(AtomicU64::new(0)),
            last_compact_revision: Arc::new(AtomicU16::new(0)),
            last_modified: Arc::new(AtomicU64::new(0)),
        };

        v.load(true, true)?;
        debug!("load volume {id} success, path: {dir}");
        Ok(v)
    }

    pub fn load(&mut self, create_if_missing: bool, load_index: bool) -> Result<(), VolumeError> {
        let _lock = self.data_file_lock.write();

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
                        .truncate(true)
                        .mode(0o644)
                        .open(&name)?;
                    info!("create volume {} data file success", self.id);
                    metadata(&name)?
                } else {
                    return Err(VolumeError::Io(err));
                }
            }
        };

        let file = if meta.permissions().readonly() {
            self.set_no_write_or_delete(true);
            fs::OpenOptions::new().read(true).open(&name)?
        } else {
            self.set_last_modified(get_time(meta.modified()?)?.as_secs());
            fs::OpenOptions::new()
                .read(true)
                .write(true)
                .mode(0o644)
                .open(&name)?
        };

        self.data_file = Some(file);

        if has_super_block {
            let super_block = self.read_super_block()?;
            self.super_block = Arc::new(super_block);
        } else {
            self.write_super_block()?;
        }

        if load_index {
            let index_file = if self.no_write_or_delete() {
                fs::OpenOptions::new()
                    .read(true)
                    .mode(0o644)
                    .open(self.index_filename())?
            } else {
                fs::OpenOptions::new()
                    .read(true)
                    .create(true)
                    .truncate(false)
                    .write(true)
                    .mode(0o644)
                    .open(self.index_filename())?
            };

            if let Err(err) = check_volume_data_integrity(self, &index_file) {
                self.set_no_write_or_delete(true);
                error!(
                    "volume data integrity checking failed. volume: {}, filename: {}, {err}",
                    self.id,
                    self.data_filename()
                );
            }

            let needle_mapper = if self.no_write_or_delete() || self.no_write_can_delete() {
                todo!("load index file from sorted file")
            } else {
                let mut needle_mapper = NeedleMapper::new(self.id, self.needle_map_type);
                needle_mapper.load_index_file(index_file)?;
                needle_mapper
            };
            self.needle_mapper = Some(needle_mapper);
            info!("load index file `{}` success", self.index_filename());
        }

        Ok(())
    }

    pub fn write_needle(&self, needle: &mut Needle) -> Result<usize, io::Error> {
        let volume_id = self.id;
        if self.readonly() {
            return Err(io::Error::new(
                ErrorKind::ReadOnlyFilesystem,
                format!("Volume {volume_id} is readonly"),
            ));
        }

        let version = self.version();

        {
            let _lock = self.data_file_lock.write();
            let file = self.data_file()?;

            let offset = append_needle_at(file)?;
            if let Err(err) = needle.append(file, offset, version) {
                error!(
                    "volume {volume_id}: write needle {} error: {err}, will do ftruncate.",
                    needle.id
                );
                ftruncate(file, offset)?;
                return Err(err);
            }

            let nv = NeedleValue {
                offset: offset.into(),
                size: needle.size,
            };
            self.set_index(needle.id, nv)?;
        }

        if self.last_modified() < needle.last_modified {
            self.set_last_modified(needle.last_modified);
        }

        Ok(needle.data_size())
    }

    pub fn delete_needle(&self, needle: &mut Needle) -> Result<usize, VolumeError> {
        if self.readonly() {
            return Err(VolumeError::Readonly(self.id));
        }

        {
            let _lock = self.data_file_lock.write();
            let mut nv = match self.get_index(needle.id)? {
                Some(nv) => nv,
                None => return Ok(0),
            };
            nv.offset = Offset(0);

            let version = self.version();
            let file = self.data_file()?;

            let offset = append_needle_at(file)?;
            needle.append(file, offset, version)?;

            self.delete_index(needle.id)?;
        }

        Ok(needle.data_size())
    }

    pub fn read_needle(&self, needle: &mut Needle) -> Result<usize, NeedleError> {
        let _lock = self.data_file_lock.read();

        match self.get_index(needle.id)? {
            Some(nv) => {
                if nv.offset == 0 || nv.size.is_deleted() {
                    return Err(NeedleError::Deleted(self.id, needle.id));
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
                Err(NeedleError::Expired(self.id, needle.id))
            }
            None => {
                error!("needle {} is not found, volume: {}", needle.id, self.id);
                Err(NeedleError::NotFound(needle.id))
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

    pub fn get_volume_info(&self) -> VolumeInfo {
        VolumeInfo {
            id: self.id,
            size: self.content_size(),
            replica_placement: self.super_block.replica_placement,
            ttl: self.super_block.ttl,
            collection: self.collection.clone(),
            version: self.version(),
            file_count: self.file_count() as i64,
            delete_count: self.deleted_count() as i64,
            delete_bytes: self.deleted_bytes(),
            read_only: self.readonly(),
        }
    }

    pub fn destroy(&self) -> Result<(), VolumeError> {
        if self.is_compacting() {
            return Err(VolumeError::Compacting(self.id));
        }

        fs::remove_file(Path::new(&self.data_filename()))?;
        fs::remove_file(Path::new(&self.index_filename()))?;

        Ok(())
    }

    pub fn data_file_size(&self) -> Result<u64, io::Error> {
        let _lock = self.data_file_lock.read();
        let file = self.data_file()?;
        Ok(file.metadata()?.len())
    }

    pub fn data_file(&self) -> Result<&File, io::Error> {
        match self.data_file.as_ref() {
            Some(data_file) => Ok(data_file),
            None => Err(io::Error::other("Data file is not loaded")),
        }
    }

    pub fn data_filename(&self) -> String {
        format!("{}.{DATA_FILE_SUFFIX}", self.filename())
    }

    pub fn index_filename(&self) -> String {
        format!("{}.{IDX_FILE_SUFFIX}", self.filename())
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

        if now().as_secs() > self.last_modified() + self.super_block.ttl.minutes() as u64 * 60 {
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

        if (ttl.minutes() as u64 + delay) * 60 + self.last_modified() < now().as_secs() {
            return true;
        }

        false
    }
}

impl Volume {
    fn write_super_block(&self) -> Result<(), io::Error> {
        let bytes = self.super_block.as_bytes();
        let file = self.data_file()?;
        if file.metadata()?.len() != 0 {
            return Ok(());
        }
        file.write_all_at(&bytes, 0)?;

        self.set_no_write_or_delete(false);
        self.set_no_write_can_delete(false);

        debug!("write super block success");
        Ok(())
    }

    fn read_super_block(&self) -> Result<SuperBlock, io::Error> {
        let mut buf = [0; SUPER_BLOCK_SIZE];
        let file = self.data_file()?;
        file.read_exact_at(&mut buf, 0)?;
        SuperBlock::parse(buf)
    }
}

impl Volume {
    fn needle_mapper(&self) -> Result<&NeedleMapper, io::Error> {
        match self.needle_mapper.as_ref() {
            Some(nm) => Ok(nm),
            None => {
                error!("needle mapper is not load, volume: {}", self.id);
                Err(io::Error::new(
                    ErrorKind::NotFound,
                    format!("Needle mapper is not loaded, volume: {}", self.id),
                ))
            }
        }
    }

    pub fn set_index(
        &self,
        key: NeedleId,
        index: NeedleValue,
    ) -> Result<Option<NeedleValue>, io::Error> {
        self.needle_mapper()?.set(key, index)
    }

    pub fn get_index(&self, key: NeedleId) -> Result<Option<NeedleValue>, io::Error> {
        Ok(self.needle_mapper()?.get(key))
    }

    pub fn delete_index(&self, key: NeedleId) -> Result<Option<NeedleValue>, io::Error> {
        self.needle_mapper()?.delete(key)
    }

    pub fn deleted_bytes(&self) -> u64 {
        let _lock = self.data_file_lock.read();
        match self.needle_mapper() {
            Ok(nm) => nm.deleted_bytes(),
            Err(_) => 0,
        }
    }

    pub fn deleted_count(&self) -> u64 {
        let _lock = self.data_file_lock.read();
        match self.needle_mapper() {
            Ok(nm) => nm.deleted_count(),
            Err(_) => 0,
        }
    }

    pub fn max_file_key(&self) -> u64 {
        let _lock = self.data_file_lock.read();
        match self.needle_mapper() {
            Ok(nm) => nm.max_file_key(),
            Err(_) => 0,
        }
    }

    pub fn file_count(&self) -> u64 {
        let _lock = self.data_file_lock.read();
        match self.needle_mapper() {
            Ok(nm) => nm.file_count(),
            Err(_) => 0,
        }
    }

    pub fn content_size(&self) -> u64 {
        let _lock = self.data_file_lock.read();
        match self.needle_mapper() {
            Ok(nm) => nm.content_size(),
            Err(_) => 0,
        }
    }

    pub fn index_file_size(&self) -> Result<u64, io::Error> {
        let _lock = self.data_file_lock.read();
        match self.needle_mapper() {
            Ok(nm) => nm.index_file_size(),
            Err(_) => Ok(0),
        }
    }
}

impl Volume {
    pub fn no_write_or_delete(&self) -> bool {
        self.no_write_or_delete.load(Ordering::Relaxed)
    }

    pub fn set_no_write_or_delete(&self, v: bool) {
        self.no_write_or_delete.store(v, Ordering::Relaxed)
    }

    pub fn no_write_can_delete(&self) -> bool {
        self.no_write_can_delete.load(Ordering::Relaxed)
    }

    pub fn set_no_write_can_delete(&self, v: bool) {
        self.no_write_can_delete.store(v, Ordering::Relaxed)
    }

    pub fn is_compacting(&self) -> bool {
        self.is_compacting.load(Ordering::Relaxed)
    }

    pub fn set_is_compacting(&self, v: bool) {
        self.is_compacting.store(v, Ordering::Relaxed)
    }

    pub fn readonly(&self) -> bool {
        self.no_write_or_delete.load(Ordering::Relaxed)
            || self.no_write_can_delete.load(Ordering::Relaxed)
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
    Io(#[from] io::Error),
    #[error("error: {0}")]
    Box(#[from] Box<dyn std::error::Error + Sync + Send>),
    #[error("Time error: {0}")]
    TimeError(#[from] TimeError),
    #[error("Bincode error: {0}")]
    Bincode(#[from] Box<bincode::ErrorKind>),
    #[error("Chrono parse error: {0}")]
    ChronoParse(#[from] chrono::ParseError),

    // parse
    #[error("Parse error: {0}")]
    Parse(#[from] ParseError),

    #[error("FromUtf8 error: {0}")]
    FromUtf8(#[from] FromUtf8Error),

    // http
    #[error("Http error: {0}")]
    Http(#[from] HttpError),

    #[error("EcVolume error: {0}")]
    EcVolume(#[from] EcVolumeError),

    #[error("Raw volume error: {0}")]
    String(String),

    #[error("Errno: {0}")]
    Errno(#[from] rustix::io::Errno),

    #[error("JoinHandle error: {0}")]
    TaskJoin(#[from] tokio::task::JoinError),

    #[error("Sequence error: {0}")]
    Sequence(#[from] SequenceError),

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
    #[error("Volume {0} is compacting.")]
    Compacting(VolumeId),
    #[error("Needle error: {0}")]
    Needle(#[from] NeedleError),
    #[error("Needle cookie not match, required cookie is {0} but got {1}")]
    NeedleCookieNotMatch(u32, u32),

    #[error("Topology error: {0}")]
    Topology(#[from] TopologyError),
    #[error("NeedleMapper is not load, volume: {0}")]
    NeedleMapperNotLoad(VolumeId),
    #[error("Volume size limit {0} exceeded, current size is {1}")]
    VolumeSizeLimit(u64, u64),
    #[error("Master not found")]
    MasterNotFound,

    // heartbeat
    #[error("Start heartbeat failed.")]
    StartHeartbeat,
    #[error("Send heartbeat to {0} failed.")]
    SendHeartbeat(FastStr),
    #[error("Leader changed, current leader is {0}, old leader is {1}")]
    LeaderChanged(FastStr, FastStr),
    #[error("Broadcast send error: {0}")]
    BroadcastSend(#[from] async_broadcast::SendError<()>),
}

impl From<VolumeError> for Status {
    fn from(value: VolumeError) -> Self {
        Status::internal(value.to_string())
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

fn append_needle_at(file: &File) -> io::Result<u64> {
    let mut offset = file.metadata()?.len();
    if offset % NEEDLE_PADDING_SIZE as u64 != 0 {
        offset = offset + (NEEDLE_PADDING_SIZE as u64 - offset % NEEDLE_PADDING_SIZE as u64);
    }
    Ok(offset)
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
    VSB: FnMut(&Arc<SuperBlock>) -> Result<(), io::Error>,
    VN: FnMut(&mut Needle, u64) -> Result<(), io::Error>,
{
    let volume = load_volume_without_index(dirname, collection, id, needle_map_type)?;
    visit_super_block(&volume.super_block)?;

    let version = volume.version();
    let mut offset = SUPER_BLOCK_SIZE as u64;

    let (mut needle, mut rest) = read_needle_header(volume.data_file()?, version, offset)?;

    let data_file = volume.data_file()?;
    loop {
        if read_needle_body {
            if let Err(err) =
                needle.read_needle_body(data_file, offset + NEEDLE_ENTRY_SIZE as u64, rest, version)
            {
                error!("cannot read needle body when scanning volume file, {err}");
            }
        }

        if let Err(err) = visit_needle(&mut needle, offset) {
            if err.kind() == ErrorKind::UnexpectedEof {
                return Ok(());
            }
        }

        offset += (NEEDLE_ENTRY_SIZE + rest) as u64;

        info!("new entry offset: {offset}");
        match read_needle_header(data_file, version, offset) {
            Ok((n, body_len)) => {
                needle = n;
                rest = body_len;
                info!(
                    "new entry needle size: {}, body length: {rest}",
                    needle.size
                );
            }
            Err(err) => {
                if err.kind() == ErrorKind::UnexpectedEof {
                    return Ok(());
                }
                return Err(VolumeError::Io(err));
            }
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::{io, sync::Arc};

    use bytes::Bytes;
    use faststr::FastStr;
    use helyim_common::{
        crc,
        ttl::Ttl,
        types::{FileId, ReplicaPlacement},
    };
    use rand::random;
    use tempfile::Builder;

    use crate::{
        needle::{Needle, NeedleMapType},
        volume::{SuperBlock, Volume, scan_volume_file},
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
            .tempdir_in("../../..")
            .unwrap();
        let dir = FastStr::new(dir.path().to_str().unwrap());
        let volume = setup(dir.clone());

        scan_volume_file(
            dir.clone(),
            FastStr::empty(),
            volume.id,
            volume.needle_map_type,
            true,
            |_super_block: &Arc<SuperBlock>| -> Result<(), io::Error> { Ok(()) },
            |needle, _offset| -> Result<(), io::Error> {
                assert_eq!(needle.data_size, 11);
                Ok(())
            },
        )
        .unwrap();
    }
}
