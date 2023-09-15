use std::{
    fmt::Display,
    fs::{self, metadata, File},
    io::{ErrorKind, Read, Seek, SeekFrom, Write},
    os::unix::fs::{FileExt, OpenOptionsExt},
    path::Path,
};

use bytes::{Buf, BufMut};
use faststr::FastStr;
use futures::{
    channel::{
        mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    StreamExt,
};
use rustix::fs::ftruncate;
use tracing::{debug, error, info};

use crate::{
    anyhow,
    errors::{Error, Result},
    rt_spawn,
    storage::{
        needle::{
            read_needle_header, Needle, NeedleValue, NEEDLE_CHECKSUM_SIZE, NEEDLE_HEADER_SIZE,
            NEEDLE_INDEX_SIZE, NEEDLE_PADDING_SIZE,
        },
        needle_map::{index_entry, NeedleMapType, NeedleMapper},
        replica_placement::ReplicaPlacement,
        ttl::Ttl,
        version::{Version, CURRENT_VERSION},
        volume_info::VolumeInfo,
        VolumeId,
    },
    util::time::{get_time, now},
};

mod vacuum;

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
    pub fn parse(buf: [u8; SUPER_BLOCK_SIZE]) -> Result<SuperBlock> {
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
    pub id: VolumeId,
    dir: FastStr,
    pub collection: FastStr,
    data_file: Option<File>,
    pub needle_mapper: NeedleMapper,
    needle_map_type: NeedleMapType,
    pub read_only: bool,
    pub super_block: SuperBlock,
    last_modified: u64,
    last_compact_index_offset: u64,
    last_compact_revision: u16,
    index_tx: Option<UnboundedSender<(u64, NeedleValue)>>,
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
            self.read_only
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
        shutdown_rx: async_broadcast::Receiver<()>,
    ) -> Result<Volume> {
        let sb = SuperBlock {
            replica_placement,
            ttl,
            ..Default::default()
        };

        let (index_tx, index_rx) = unbounded();

        let mut v = Volume {
            id,
            dir: dir.clone(),
            collection,
            super_block: sb,
            data_file: None,
            needle_map_type,
            index_tx: Some(index_tx),
            needle_mapper: NeedleMapper::default(),
            read_only: false,
            last_compact_index_offset: 0,
            last_compact_revision: 0,
            last_modified: 0,
        };

        v.load(true, true)?;
        v.spawn_index_file_writer(index_rx, shutdown_rx)?;
        debug!("load volume {id} success, path: {dir}");
        Ok(v)
    }

    pub fn load(&mut self, create_if_missing: bool, load_index: bool) -> Result<()> {
        if self.data_file.is_some() {
            return Err(anyhow!("volume {} has loaded!", self.id));
        }

        let name = self.data_filename();
        debug!("loading volume: {}", name);

        let meta = match metadata(&name) {
            Ok(m) => m,
            Err(err) => {
                debug!("get metadata err: {err}");
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
                    return Err(Error::Io(err));
                }
            }
        };

        if meta.permissions().readonly() {
            let file = fs::OpenOptions::new().read(true).open(&name)?;
            self.data_file = Some(file);
            self.read_only = true;
        } else {
            let file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .mode(0o644)
                .open(&name)?;

            self.last_modified = get_time(meta.modified()?)?.as_secs();
            self.data_file = Some(file);
        }

        self.write_super_block()?;
        self.read_super_block()?;

        if load_index {
            let index_file = fs::OpenOptions::new()
                .read(true)
                .create(true)
                .write(true)
                .open(self.index_filename())?;

            if let Err(err) = check_volume_data_integrity(self, &index_file) {
                self.read_only = true;
                error!(
                    "volume data integrity checking failed. volume: {}, filename: {}, {err}",
                    self.id,
                    self.data_filename()
                );
            }

            self.needle_mapper.load_idx_file(index_file)?;
            info!("load index file `{}` success", self.index_filename());
        }

        Ok(())
    }

    pub async fn write_needle(&mut self, mut needle: Needle) -> Result<Needle> {
        if self.read_only {
            return Err(anyhow!("data file {} is read only", self.data_filename()));
        }

        let version = self.version();
        let file = self.file_mut()?;

        let mut offset = file.seek(SeekFrom::End(0))?;
        if offset % NEEDLE_PADDING_SIZE as u64 != 0 {
            offset = offset + (NEEDLE_PADDING_SIZE as u64 - offset % NEEDLE_PADDING_SIZE as u64);
            offset = file.seek(SeekFrom::Start(offset))?;
        }
        offset /= NEEDLE_PADDING_SIZE as u64;

        if let Err(err) = needle.append(file, version) {
            error!("write needle {} error: {err}, will do ftruncate.", needle);
            ftruncate(file, offset)?;
            return Err(err);
        }

        let nv = NeedleValue {
            offset: offset as u32,
            size: needle.size,
        };
        self.needle_mapper.set(needle.id, nv);
        self.write_index_file(needle.id, nv).await?;

        if self.last_modified < needle.last_modified {
            self.last_modified = needle.last_modified;
        }

        Ok(needle)
    }

    pub async fn delete_needle(&mut self, mut needle: Needle) -> Result<u32> {
        if self.read_only {
            return Err(anyhow!("{} is read only", self.data_filename()));
        }

        let mut nv = match self.needle_mapper.get(needle.id) {
            Some(nv) => nv,
            None => return Ok(0),
        };
        nv.offset = 0;

        self.needle_mapper.set(needle.id, nv);
        self.write_index_file(needle.id, nv).await?;

        needle.set_is_delete();
        needle.data.clear();

        let version = self.version();
        let file = self.file_mut()?;
        needle.append(file, version)?;
        Ok(nv.size)
    }

    pub fn read_needle(&mut self, mut needle: Needle) -> Result<Needle> {
        match self.needle_mapper.get(needle.id) {
            Some(nv) => {
                if nv.offset == 0 {
                    return Err(anyhow!("needle {} already deleted", needle.id));
                }

                let version = self.version();
                let data_file = self.file_mut()?;
                needle.read_data(data_file, nv.offset, nv.size, version)?;

                if needle.has_ttl() && needle.has_last_modified_date() {
                    let minutes = needle.ttl.minutes();
                    if minutes > 0
                        && now().as_secs() >= (needle.last_modified + minutes as u64 * 60)
                    {
                        return Err(anyhow!("needle {} has expired", needle.id));
                    }
                }
                Ok(needle)
            }
            None => Err(anyhow!("needle {} not found", needle.id)),
        }
    }

    pub fn file(&self) -> Result<&File> {
        match self.data_file.as_ref() {
            Some(data_file) => Ok(data_file),
            None => Err(anyhow!("volume {} not load", self.id)),
        }
    }

    pub fn file_mut(&mut self) -> Result<&mut File> {
        match self.data_file.as_mut() {
            Some(data_file) => Ok(data_file),
            None => Err(anyhow!("volume {} not load", self.id)),
        }
    }

    pub fn version(&self) -> Version {
        self.super_block.version
    }

    pub fn data_filename(&self) -> String {
        format!("{}.{DATA_FILE_SUFFIX}", self.filename())
    }

    pub fn index_filename(&self) -> String {
        format!("{}.{IDX_FILE_SUFFIX}", self.filename())
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
            read_only: self.read_only,
        }
    }

    pub fn destroy(self) -> Result<()> {
        if self.read_only {
            return Err(anyhow!("{} is read only", self.data_filename()));
        }

        fs::remove_file(Path::new(&self.data_filename()))?;
        fs::remove_file(Path::new(&self.index_filename()))?;

        Ok(())
    }

    pub fn content_size(&self) -> u64 {
        self.needle_mapper.content_size()
    }

    pub fn deleted_bytes(&self) -> u64 {
        self.needle_mapper.deleted_bytes()
    }

    pub fn deleted_count(&self) -> u64 {
        self.needle_mapper.deleted_count()
    }

    pub fn size(&self) -> Result<u64> {
        let file = self.file()?;
        Ok(file.metadata()?.len())
    }

    // volume is expired if modified time + volume ttl < now
    // except when volume is empty
    // or when the volume does not have a ttl
    // or when volumeSizeLimit is 0 when server just starts
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

    // wait either maxDelayMinutes or 10% of ttl minutes
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
    fn spawn_index_file_writer(
        &self,
        mut index_rx: UnboundedReceiver<(u64, NeedleValue)>,
        mut shutdown_rx: async_broadcast::Receiver<()>,
    ) -> Result<()> {
        let mut file = fs::OpenOptions::new()
            // most hardware designs cannot support write permission without read permission
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(self.index_filename())?;

        let vid = self.id;
        rt_spawn(async move {
            info!("index file writer starting, volume: {vid}");
            let mut buf = vec![];
            loop {
                tokio::select! {
                    Some((key, value)) = index_rx.next() => {
                        buf.put_u64(key);
                        buf.put_u32(value.offset);
                        buf.put_u32(value.size);

                        match file.write_all(&buf) {
                            Ok(_) => buf.clear(),
                            Err(err) => error!("failed to write index file, volume {vid}, error: {err}"),
                        }
                    },
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
            info!("index file writer stopped, volume: {vid}");
        });

        Ok(())
    }

    fn write_super_block(&mut self) -> Result<()> {
        let mut file = self.file()?;
        let meta = file.metadata()?;

        if meta.len() != 0 {
            return Ok(());
        }

        let bytes = self.super_block.as_bytes();
        file.write_all(&bytes)?;
        debug!("write super block success");
        Ok(())
    }

    fn read_super_block(&mut self) -> Result<()> {
        let mut buf = [0; SUPER_BLOCK_SIZE];
        {
            let file = self.file_mut()?;
            file.seek(SeekFrom::Start(0))?;
            file.read_exact(&mut buf)?;
        }
        self.super_block = SuperBlock::parse(buf)?;

        Ok(())
    }

    async fn write_index_file(&mut self, key: u64, value: NeedleValue) -> Result<()> {
        if let Some(index_tx) = self.index_tx.as_ref() {
            index_tx.unbounded_send((key, value))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct VolumeEventTx(UnboundedSender<VolumeEvent>);

pub enum VolumeEvent {
    Load {
        create_if_missing: bool,
        load_index: bool,
        tx: oneshot::Sender<Result<()>>,
    },
    WriteNeedle {
        needle: Needle,
        tx: oneshot::Sender<Result<Needle>>,
    },
    DeleteNeedle {
        needle: Needle,
        tx: oneshot::Sender<Result<u32>>,
    },
    ReadNeedle {
        needle: Needle,
        tx: oneshot::Sender<Result<Needle>>,
    },
    Version {
        tx: oneshot::Sender<Result<Version>>,
    },
    DataFile {
        tx: oneshot::Sender<Result<File>>,
    },
    Filename {
        tx: oneshot::Sender<Result<String>>,
    },
    DataFilename {
        tx: oneshot::Sender<Result<String>>,
    },
    IndexFilename {
        tx: oneshot::Sender<Result<String>>,
    },
    VolumeInfo {
        tx: oneshot::Sender<VolumeInfo>,
    },
    Destroy {
        tx: oneshot::Sender<Result<()>>,
    },
    ContentSize {
        tx: oneshot::Sender<u64>,
    },
    DeletedBytes {
        tx: oneshot::Sender<u64>,
    },
    DeletedCount {
        tx: oneshot::Sender<u64>,
    },
    Size {
        tx: oneshot::Sender<Result<u64>>,
    },
    Expired {
        volume_size_limit: u64,
        tx: oneshot::Sender<bool>,
    },
    ExpiredLongEnough {
        volume_size_limit: u64,
        tx: oneshot::Sender<bool>,
    },
    NeedToReplicate {
        tx: oneshot::Sender<bool>,
    },
}

fn load_volume_without_index(
    dirname: FastStr,
    collection: FastStr,
    id: VolumeId,
    needle_map_type: NeedleMapType,
) -> Result<Volume> {
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
) -> Result<()>
where
    VSB: FnMut(&mut SuperBlock) -> Result<()>,
    VN: FnMut(&mut Needle, u32) -> Result<()>,
{
    let mut volume = load_volume_without_index(dirname, collection, id, needle_map_type)?;
    visit_super_block(&mut volume.super_block)?;

    let version = volume.version();
    let mut offset = SUPER_BLOCK_SIZE as u32;

    let (mut needle, mut rest) = read_needle_header(volume.file()?, version, offset)?;

    loop {
        if read_needle_body {
            needle.read_needle_body(
                volume.file_mut()?,
                offset + NEEDLE_HEADER_SIZE,
                rest,
                version,
            )?;
            if needle.data_size >= needle.size {
                // this should come from a bug reported on #87 and #93
                // fixed in v0.69
                // remove this whole "if" clause later, long after 0.69
                let padding = NEEDLE_PADDING_SIZE
                    - ((needle.size + NEEDLE_HEADER_SIZE + NEEDLE_CHECKSUM_SIZE)
                        % NEEDLE_PADDING_SIZE);
                needle.size = 0;
                rest = needle.size + NEEDLE_CHECKSUM_SIZE + padding;
                if rest % NEEDLE_PADDING_SIZE != 0 {
                    rest += NEEDLE_PADDING_SIZE - rest % NEEDLE_PADDING_SIZE;
                }
            }
        }

        visit_needle(&mut needle, offset)?;
        offset += NEEDLE_HEADER_SIZE + rest;

        match read_needle_header(volume.file()?, version, offset) {
            Ok((n, body_len)) => {
                needle = n;
                rest = body_len;
            }
            Err(Error::Io(err)) => {
                if err.kind() == ErrorKind::UnexpectedEof {
                    return Ok(());
                }
            }
            Err(err) => {
                error!("read needle header error, {err}");
                return Err(anyhow!("read needle header error, {err}"));
            }
        }
    }
}

// volume checking start

pub fn verify_index_file_integrity(index_file: &File) -> Result<u64> {
    let meta = index_file.metadata()?;
    let size = meta.len();
    if size % NEEDLE_PADDING_SIZE as u64 != 0 {
        return Err(Error::DataIntegrity(format!(
            "index file's size is {size} bytes, maybe corrupted"
        )));
    }
    Ok(size)
}

pub fn check_volume_data_integrity(volume: &mut Volume, index_file: &File) -> Result<()> {
    let index_size = verify_index_file_integrity(index_file)?;
    if index_size == 0 {
        return Ok(());
    }
    let last_index_entry =
        read_index_entry_at_offset(index_file, index_size - NEEDLE_INDEX_SIZE as u64)?;
    let (key, offset, size) = index_entry(&last_index_entry);
    if offset == 0 {
        return Ok(());
    }
    let version = volume.version();
    verify_needle_integrity(volume.file_mut()?, version, key, offset, size)
}

pub fn read_index_entry_at_offset(index_file: &File, offset: u64) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; NEEDLE_INDEX_SIZE as usize];
    index_file.read_exact_at(&mut buf, offset)?;
    Ok(buf)
}

pub fn verify_needle_integrity(
    data_file: &mut File,
    version: Version,
    key: u64,
    offset: u32,
    size: u32,
) -> Result<()> {
    let mut needle = Needle::default();
    needle.read_data(data_file, offset, size, version)?;
    if needle.id != key {
        return Err(Error::DataIntegrity(format!(
            "index key {key} does not match needle's id {}",
            needle.id
        )));
    }
    Ok(())
}

// volume checking end

#[cfg(test)]
mod tests {
    use std::path::Path;

    use async_broadcast::broadcast;
    use bytes::Bytes;
    use faststr::FastStr;

    use crate::storage::{
        volume::{check_volume_data_integrity, Volume},
        Needle, NeedleMapType, ReplicaPlacement, Ttl,
    };

    #[tokio::test]
    pub async fn test_check_volume_data_integrity() {
        let (_shutdown, shutdown_rx) = broadcast(1);
        let path = Path::new("/tmp/helyim");
        if path.exists() {
            std::fs::remove_dir_all("/tmp/helyim").unwrap();
        }
        std::fs::create_dir("/tmp/helyim").unwrap();

        let mut volume = Volume::new(
            FastStr::from_static_str("/tmp/helyim/"),
            FastStr::empty(),
            1,
            NeedleMapType::NeedleMapInMemory,
            ReplicaPlacement::default(),
            Ttl::default(),
            0,
            shutdown_rx,
        )
        .unwrap();

        let fid = "1b1f52120";
        let mut needle = Needle {
            data: Bytes::from_static(b"Hello world"),
            ..Default::default()
        };
        needle.parse_path(fid).unwrap();
        volume.write_needle(&mut needle).await.unwrap();

        let index_file = std::fs::OpenOptions::new()
            .read(true)
            .open(volume.index_filename())
            .unwrap();

        assert!(check_volume_data_integrity(&mut volume, &index_file).is_ok());
    }
}
