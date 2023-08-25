use std::{
    fmt::Display,
    fs::{self, metadata, File},
    io::{BufWriter, ErrorKind, Read, Seek, SeekFrom, Write},
    os::unix::fs::OpenOptionsExt,
    path::Path,
    time::Duration,
};

use bytes::{Buf, BufMut};
use futures::{
    channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    SinkExt, StreamExt,
};
use tokio::sync::broadcast;
use tracing::{debug, error, info};

use crate::{
    anyhow,
    errors::{Error, Result},
    rt_spawn,
    storage::{
        needle::{Needle, NeedleValue, NEEDLE_PADDING_SIZE},
        needle_map::{NeedleMapType, NeedleMapper},
        replica_placement::ReplicaPlacement,
        ttl::Ttl,
        version::{Version, CURRENT_VERSION},
        volume_info::VolumeInfo,
        VolumeId,
    },
    util::time::{get_time, now},
};

pub const SUPER_BLOCK_SIZE: usize = 8;

pub(crate) const DATA_FILE_SUFFIX: &str = "dat";
const IDX_FILE_SUFFIX: &str = "idx";

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
            // TODO
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

pub struct Volume {
    pub id: VolumeId,
    pub dir: String,
    pub collection: String,
    data_file: Option<File>,
    pub needle_mapper: NeedleMapper,
    pub needle_map_type: NeedleMapType,
    pub read_only: bool,
    pub super_block: SuperBlock,
    pub last_modified: u64,
    pub last_compact_index_offset: u64,
    pub last_compact_revision: u16,
    index_tx: UnboundedSender<(u64, NeedleValue)>,

    shutdown: broadcast::Sender<()>,
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

fn write_index_file<W: Write>(buf: &mut Vec<u8>, writer: &mut W) -> Result<()> {
    if !buf.is_empty() {
        writer.write_all(buf)?;
        writer.flush()?;
        buf.clear();
    }
    Ok(())
}

impl Volume {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        dir: &str,
        collection: &str,
        id: VolumeId,
        needle_map_type: NeedleMapType,
        replica_placement: ReplicaPlacement,
        ttl: Ttl,
        _preallocate: i64,
        shutdown: broadcast::Sender<()>,
    ) -> Result<Volume> {
        debug!("new volume dir: {}, id: {}", dir, id);
        let sb = SuperBlock {
            replica_placement,
            ttl,
            ..Default::default()
        };

        let (index_tx, index_rx) = unbounded();

        let mut v = Volume {
            id,
            dir: dir.to_string(),
            collection: collection.to_string(),
            super_block: sb,
            data_file: None,
            needle_map_type,
            index_tx,
            needle_mapper: NeedleMapper::default(),
            read_only: false,
            last_compact_index_offset: 0,
            last_compact_revision: 0,
            last_modified: 0,
            shutdown,
        };

        v.load(true, true)?;
        v.spawn_index_file_writer(index_rx)?;
        debug!("new volume dir: {}, id: {} load success", dir, id);
        Ok(v)
    }

    fn spawn_index_file_writer(
        &self,
        mut index_rx: UnboundedReceiver<(u64, NeedleValue)>,
    ) -> Result<()> {
        let file = fs::OpenOptions::new()
            // most hardware designs cannot support write permission without read permission
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(self.index_file_name())?;

        let vid = self.id;
        let mut writer = BufWriter::new(file);

        let mut shutdown_rx = self.shutdown.subscribe();
        rt_spawn(async move {
            let mut buf = vec![];
            let mut interval = tokio::time::interval(Duration::from_secs(1));

            loop {
                tokio::select! {
                    Some((key, value)) = index_rx.next() => {
                        buf.put_u64(key);
                        buf.put_u32(value.offset);
                        buf.put_u32(value.size);
                    },
                    _ = interval.tick() => {
                        if let Err(err) = write_index_file(&mut buf, &mut writer) {
                            error!("failed to write index file via mmap, volume {vid}, error: {err}");
                            break;
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
            if let Err(err) = write_index_file(&mut buf, &mut writer) {
                error!("failed to write index file via mmap, volume {vid}, error: {err}");
            }
            info!("index file writer stopped, volume: {}", vid);
        });

        Ok(())
    }

    fn load(&mut self, create_if_missing: bool, load_index: bool) -> Result<()> {
        if self.data_file.is_some() {
            return Err(anyhow!("volume {} has loaded!", self.id));
        }

        let name = self.data_file_name();
        debug!("loading volume: {}", name);

        let meta = match metadata(&name) {
            Ok(m) => m,
            Err(err) => {
                debug!("get metadata err: {}", err);
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
                    return Err(Error::from(err));
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
                .open(self.index_file_name())?;

            self.needle_mapper.load_idx_file(&index_file)?;
            info!("load index file `{}` success", self.index_file_name());
        }

        Ok(())
    }

    fn write_super_block(&mut self) -> Result<()> {
        let bytes = self.super_block.as_bytes();
        let file = self.file_mut()?;
        let meta = file.metadata()?;

        if meta.len() != 0 {
            return Ok(());
        }

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
        Ok(self.index_tx.send((key, value)).await?)
    }

    pub async fn write_needle(&mut self, n: &mut Needle) -> Result<u32> {
        if self.read_only {
            return Err(anyhow!("data file {} is read-only", self.data_file_name()));
        }

        let mut offset: u64;
        let size: u32;
        let version = self.version();

        {
            let file = self.file_mut()?;
            offset = file.seek(SeekFrom::End(0))?;

            if offset % NEEDLE_PADDING_SIZE as u64 != 0 {
                offset =
                    offset + (NEEDLE_PADDING_SIZE as u64 - offset % NEEDLE_PADDING_SIZE as u64);
                offset = file.seek(SeekFrom::Start(offset))?;
            }

            size = match n.append(file, version) {
                Ok(s) => s.0,
                Err(err) => {
                    // TODO
                    // truncate file
                    return Err(err);
                }
            };
        }

        offset /= NEEDLE_PADDING_SIZE as u64;
        if n.is_delete() {
            self.needle_mapper.set(
                n.id,
                NeedleValue {
                    offset: 0,
                    size: n.size,
                },
            );
        } else {
            self.needle_mapper.set(
                n.id,
                NeedleValue {
                    offset: offset as u32,
                    size: n.size,
                },
            );
        }

        self.write_index_file(
            n.id,
            NeedleValue {
                offset: offset as u32,
                size: n.size,
            },
        )
        .await?;

        if self.last_modified < n.last_modified {
            self.last_modified = n.last_modified;
        }

        Ok(size)
    }

    pub async fn delete_needle(&mut self, n: &mut Needle) -> Result<u32> {
        if self.read_only {
            return Err(anyhow!("{} is read only", self.data_file_name()));
        }

        let nv = match self.needle_mapper.get(n.id) {
            Some(nv) => nv,
            None => return Ok(0),
        };

        n.set_is_delete();
        n.data.clear();
        self.write_needle(n).await?;

        Ok(nv.size)
    }

    pub fn read_needle(&mut self, n: &mut Needle) -> Result<u32> {
        match self.needle_mapper.get(n.id) {
            Some(nv) => {
                if nv.offset == 0 {
                    return Err(anyhow!("needle {} already deleted", n.id));
                }

                let version = self.version();
                let data_file = self.file_mut()?;
                n.read_data(data_file, nv.offset, nv.size, version)?;

                if n.has_ttl() && n.has_last_modified_date() {
                    let minutes = n.ttl.minutes();
                    if minutes > 0 && now().as_secs() >= (n.last_modified + minutes as u64 * 60) {
                        return Err(anyhow!("needle {} has expired", n.id));
                    }
                }
                Ok(n.data.len() as u32)
            }
            None => Err(anyhow!("needle {} not found", n.id)),
        }
    }

    fn file(&self) -> Result<&File> {
        match self.data_file.as_ref() {
            Some(data_file) => Ok(data_file),
            None => Err(anyhow!("volume {} not load", self.id)),
        }
    }

    fn file_mut(&mut self) -> Result<&mut File> {
        match self.data_file.as_mut() {
            Some(data_file) => Ok(data_file),
            None => Err(anyhow!("volume {} not load", self.id)),
        }
    }

    fn version(&self) -> Version {
        self.super_block.version
    }

    pub fn data_file_name(&self) -> String {
        format!("{}.{}", self.file_name(), DATA_FILE_SUFFIX)
    }

    pub fn index_file_name(&self) -> String {
        format!("{}.{}", self.file_name(), IDX_FILE_SUFFIX)
    }

    pub fn file_name(&self) -> String {
        let mut rt = self.dir.clone();
        if !rt.ends_with('/') {
            rt.push('/');
        }
        if self.collection.is_empty() {
            format!("{}{}", rt, self.id)
        } else {
            format!("{}{}_{}", rt, self.collection, self.id)
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
            delete_count: self.needle_mapper.delete_count() as i64,
            delete_byte_count: self.needle_mapper.deleted_byte_count(),
            read_only: self.read_only,
        }
    }

    pub fn destroy(&mut self) -> Result<()> {
        if self.read_only {
            return Err(anyhow!("{} is read only", self.data_file_name()));
        }

        fs::remove_file(Path::new(&self.data_file_name()))?;
        fs::remove_file(Path::new(&self.index_file_name()))?;

        Ok(())
    }

    /// the volume file size
    pub fn content_size(&self) -> u64 {
        self.needle_mapper.content_size()
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
        self.super_block.replica_placement.get_copy_count() > 1
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
