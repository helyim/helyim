use std::{
    collections::HashMap,
    fs,
    fs::File,
    io::{ErrorKind, Read, Seek, SeekFrom, Write},
    os::unix::fs::{FileExt, OpenOptionsExt},
    sync::Arc,
    time::SystemTime,
};

use bytes::{Buf, BufMut};
use faststr::FastStr;
use futures::channel::mpsc::UnboundedSender;
use helyim_proto::VolumeInfo;
use parking_lot::Mutex;

use crate::{
    errors::Result,
    proto::{maybe_load_volume_info, save_volume_info},
    storage::{
        erasure_coding::locate::{locate_data, Interval},
        index_entry,
        needle::{
            actual_offset, actual_size, NEEDLE_HEADER_SIZE, NEEDLE_ID_SIZE, NEEDLE_MAP_ENTRY_SIZE,
            OFFSET_SIZE, SIZE_SIZE, TOMBSTONE_FILE_SIZE,
        },
        version::{Version, VERSION3},
        NeedleError, NeedleId, NeedleValue, VolumeId,
    },
    util::file_exists,
};

mod decoder;
pub use decoder::{find_data_filesize, write_data_file, write_idx_file_from_ec_index};

mod encoder;
pub use encoder::{rebuild_ec_files, write_ec_files, write_sorted_file_from_idx};

use crate::errors::Error;

mod locate;
mod volume_info;

pub type ShardId = u8;

pub const DATA_SHARDS_COUNT: u32 = 10;
pub const PARITY_SHARDS_COUNT: u32 = 4;
pub const TOTAL_SHARDS_COUNT: u32 = DATA_SHARDS_COUNT + PARITY_SHARDS_COUNT;
pub const ERASURE_CODING_LARGE_BLOCK_SIZE: u64 = 1024 * 1024 * 1024;
pub const ERASURE_CODING_SMALL_BLOCK_SIZE: u64 = 1024 * 1024;

pub struct EcVolume {
    volume_id: VolumeId,
    dir: FastStr,
    collection: FastStr,
    ecx_file: File,
    ecx_filesize: u64,
    ecx_created_at: SystemTime,
    shards: Vec<EcVolumeShard>,
    shard_locations: HashMap<ShardId, Vec<FastStr>>,
    shard_locations_refresh_time: SystemTime,
    version: Version,
    ecj_file: Arc<Mutex<File>>,
}

impl EcVolume {
    pub fn new(dir: FastStr, collection: FastStr, vid: VolumeId) -> Result<EcVolume> {
        let base_filename = ec_shard_filename(&collection, &dir, vid);
        let ecx_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .mode(0o644)
            .open(format!("{}.ecx", base_filename))?;
        let ecx_filesize = ecx_file.metadata()?.len();
        let ecx_created_at = ecx_file.metadata()?.created()?;

        let ecj_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o644)
            .open(format!("{}.ecj", base_filename))?;

        let mut version = VERSION3;
        let filename = format!("{}.vif", base_filename);
        if let Some(volume_info) = maybe_load_volume_info(&filename)? {
            version = volume_info.version as Version;
        } else {
            let volume_info = VolumeInfo {
                version: version as u32,
                ..Default::default()
            };
            save_volume_info(&filename, volume_info)?;
        }

        Ok(EcVolume {
            volume_id: vid,
            dir,
            collection,
            ecx_file,
            ecx_filesize,
            ecx_created_at,
            ecj_file: Arc::new(Mutex::new(ecj_file)),
            shards: Vec::new(),
            shard_locations: HashMap::new(),
            shard_locations_refresh_time: SystemTime::now(),
            version,
        })
    }

    pub fn add_shard(&mut self, shard: EcVolumeShard) -> bool {
        for item in self.shards.iter() {
            if shard.shard_id == item.shard_id {
                return false;
            }
        }

        self.shards.push(shard);
        self.shards.sort_by(|left, right| {
            left.volume_id
                .cmp(&right.volume_id)
                .then(left.shard_id.cmp(&right.shard_id))
        });
        true
    }

    pub fn delete_shard(&mut self, shard_id: ShardId) -> Option<EcVolumeShard> {
        let mut idx = None;
        for (i, shard) in self.shards.iter().enumerate() {
            if shard.shard_id == shard_id {
                idx = Some(i);
            }
        }
        idx.map(|idx| self.shards.remove(idx))
    }

    pub fn find_shard(&self, shard_id: ShardId) -> Option<&EcVolumeShard> {
        self.shards.iter().find(|shard| shard.shard_id == shard_id)
    }

    pub fn locate_ec_shard_needle<F>(
        &self,
        needle_id: NeedleId,
        version: Version,
    ) -> Result<(NeedleValue, Vec<Interval>)>
    where
        F: FnMut(&File, u64) -> Result<()>,
    {
        let needle_value = self.find_needle_from_ecx::<F>(needle_id)?;
        let shard = &self.shards[0];
        let intervals = locate_data(
            ERASURE_CODING_LARGE_BLOCK_SIZE,
            ERASURE_CODING_SMALL_BLOCK_SIZE,
            shard.ecd_filesize * DATA_SHARDS_COUNT as u64,
            actual_offset(needle_value.offset),
            actual_size(needle_value.size),
        );
        Ok((needle_value, intervals))
    }

    pub fn find_needle_from_ecx<F>(&self, needle_id: NeedleId) -> Result<NeedleValue>
    where
        F: FnMut(&File, u64) -> Result<()>,
    {
        search_needle_from_sorted_index::<F>(&self.ecx_file, self.ecx_filesize, needle_id, None)
    }

    pub fn delete_needle_from_ecx(&mut self, needle_id: NeedleId) -> Result<()> {
        search_needle_from_sorted_index(
            &self.ecx_file,
            self.ecx_filesize,
            needle_id,
            Some(mark_needle_deleted),
        )?;

        let mut buf = vec![0u8; NEEDLE_ID_SIZE as usize];
        buf.put_u64(needle_id);

        let mut ecj_file = self.ecj_file.lock();
        ecj_file.seek(SeekFrom::End(0))?;
        ecj_file.write_all(&buf)?;
        Ok(())
    }

    fn rebuild_ecx_file(base_filename: &str) -> Result<()> {
        let ecj_filename = format!("{}.ecj", base_filename);
        if !file_exists(&ecj_filename)? {
            return Ok(());
        }
        let ecx_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .mode(0o644)
            .open(format!("{}.ecx", base_filename))?;
        let metadata = ecx_file.metadata()?;
        let ecx_filesize = metadata.len();

        let mut ecj_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .mode(0o644)
            .open(&ecj_filename)?;
        let mut buf = [0u8; NEEDLE_ID_SIZE as usize];

        loop {
            if let Err(err) = ecj_file.read_exact(&mut buf) {
                if err.kind() == ErrorKind::UnexpectedEof {
                    break;
                }
            }
            let needle_id = (&buf[..]).get_u64();
            search_needle_from_sorted_index(
                &ecx_file,
                ecx_filesize,
                needle_id,
                Some(mark_needle_deleted),
            )?;
        }

        fs::remove_file(&ecj_filename)?;
        Ok(())
    }

    pub fn filename(&self) -> String {
        ec_shard_filename(&self.collection, &self.dir, self.volume_id)
    }

    pub fn destroy(self) -> Result<()> {
        let filename = self.filename();
        for shard in self.shards {
            shard.destroy()?;
        }
        fs::remove_file(format!("{}.ecx", filename))?;
        fs::remove_file(format!("{}.ecj", filename))?;
        fs::remove_file(format!("{}.vif", filename))?;
        Ok(())
    }
}

fn search_needle_from_sorted_index<F>(
    ecx_file: &File,
    ecx_filesize: u64,
    needle_id: NeedleId,
    process_needle: Option<F>,
) -> Result<NeedleValue>
where
    F: FnMut(&File, u64) -> Result<()>,
{
    let mut buf = [0u8; NEEDLE_MAP_ENTRY_SIZE as usize];
    let (mut low, mut high) = (0u64, ecx_filesize / NEEDLE_MAP_ENTRY_SIZE as u64);
    while low < high {
        let middle = (low + high) / 2;
        ecx_file.read_at(&mut buf, middle * NEEDLE_MAP_ENTRY_SIZE as u64)?;
        let (key, offset, size) = index_entry(&buf);
        if key == needle_id {
            if let Some(mut process_needle) = process_needle {
                process_needle(ecx_file, middle * NEEDLE_HEADER_SIZE as u64)?;
            }
            return Ok(NeedleValue { offset, size });
        }
        if key < needle_id {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    Err(NeedleError::NotFound(0, needle_id).into())
}

pub struct EcVolumeShard {
    shard_id: ShardId,
    volume_id: VolumeId,
    collection: FastStr,
    dir: FastStr,
    ecd_file: File,
    ecd_filesize: u64,
}

impl EcVolumeShard {
    pub fn new(
        dir: FastStr,
        collection: FastStr,
        volume_id: VolumeId,
        id: ShardId,
    ) -> Result<Self> {
        let base_filename = ec_shard_filename(&collection, &dir, volume_id);
        let ecd_filename = format!("{}{}", base_filename, to_ext(id));
        let ecd_file = fs::OpenOptions::new()
            .read(true)
            .mode(0o644)
            .open(ecd_filename)?;
        let ecd_filesize = ecd_file.metadata()?.len();
        Ok(EcVolumeShard {
            shard_id: id,
            volume_id,
            collection,
            dir,
            ecd_file,
            ecd_filesize,
        })
    }

    pub fn filename(&self) -> String {
        ec_shard_filename(&self.collection, &self.dir, self.volume_id)
    }

    pub fn destroy(self) -> Result<()> {
        fs::remove_file(format!("{}{}", self.filename(), to_ext(self.shard_id)))?;
        Ok(())
    }
}

pub fn ec_shard_filename(collection: &str, dir: &str, volume_id: VolumeId) -> String {
    if collection.is_empty() {
        format!("{}{}", dir, volume_id)
    } else {
        format!("{}{}_{}", dir, collection, volume_id)
    }
}

pub fn ec_shard_base_filename(collection: &str, volume_id: VolumeId) -> String {
    if collection.is_empty() {
        format!("{}", volume_id)
    } else {
        format!("{}_{}", collection, volume_id)
    }
}

pub fn to_ext(ec_idx: ShardId) -> String {
    format!(".ec{:02}", ec_idx)
}

fn mark_needle_deleted(file: &File, offset: u64) -> Result<()> {
    let mut buf = vec![0u8; SIZE_SIZE as usize];
    buf.put_i32(TOMBSTONE_FILE_SIZE);
    file.write_all_at(&buf, offset + NEEDLE_ID_SIZE as u64 + OFFSET_SIZE as u64)?;
    Ok(())
}

pub fn rebuild_ecx_file(base_filename: &str) -> Result<()> {
    let ecj_filename = format!("{}.ecj", base_filename);
    if !file_exists(&ecj_filename)? {
        return Ok(());
    }
    let ecx_file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .mode(0o644)
        .open(format!("{}.ecx", base_filename))?;
    let ecx_filesize = ecx_file.metadata()?.len();

    let mut ecj_file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .mode(0o644)
        .open(format!("{}.ecj", base_filename))?;
    let mut buf = vec![0u8; NEEDLE_ID_SIZE as usize];

    loop {
        if ecj_file.read_exact(&mut buf).is_err() {
            break;
        }
        let needle_id = (&buf[..]).get_u64();
        if let Err(err) = search_needle_from_sorted_index(
            &ecx_file,
            ecx_filesize,
            needle_id,
            Some(mark_needle_deleted),
        ) {
            if !matches!(err, Error::Needle(NeedleError::NotFound(_, _))) {
                return Err(err);
            }
        }
    }

    fs::remove_file(ecj_filename)?;

    Ok(())
}

#[derive(Debug, Clone)]
pub struct EcVolumeEventTx(UnboundedSender<EcVolumeEvent>);

impl EcVolumeEventTx {
    pub fn new(tx: UnboundedSender<EcVolumeEvent>) -> Self {
        Self(tx)
    }

    pub fn destroy(&self) -> Result<()> {
        self.0.unbounded_send(EcVolumeEvent::Destroy)?;
        Ok(())
    }
}

pub enum EcVolumeEvent {
    Destroy,
}
