use std::{
    collections::HashMap,
    fs,
    fs::File,
    io::{ErrorKind, Read, Seek, SeekFrom, Write},
    os::unix::fs::OpenOptionsExt,
    sync::Arc,
    time::SystemTime,
};

use bytes::{Buf, BufMut};
use faststr::FastStr;
use helyim_macros::event_fn;
use helyim_proto::{VolumeEcShardInformationMessage, VolumeInfo};
use parking_lot::Mutex;

use crate::{
    errors::Result,
    proto::{maybe_load_volume_info, save_volume_info},
    storage::{
        erasure_coding::{
            ec_shard_filename,
            locate::{locate_data, Interval},
            mark_needle_deleted, search_needle_from_sorted_index, EcVolumeShard, ShardId,
            DATA_SHARDS_COUNT, ERASURE_CODING_LARGE_BLOCK_SIZE, ERASURE_CODING_SMALL_BLOCK_SIZE,
        },
        needle::{actual_offset, actual_size, NEEDLE_ID_SIZE},
        version::{Version, VERSION3},
        NeedleId, NeedleValue, VolumeId,
    },
    util::file_exists,
};

pub struct EcVolume {
    pub volume_id: VolumeId,
    dir: FastStr,
    collection: FastStr,
    ecx_file: File,
    ecx_filesize: u64,
    ecx_created_at: SystemTime,
    shards: Vec<Arc<EcVolumeShard>>,
    pub shard_locations: HashMap<ShardId, Vec<FastStr>>,
    shard_locations_refresh_time: SystemTime,
    version: Version,
    ecj_file: Arc<Mutex<File>>,
}

#[event_fn]
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

        self.shards.push(Arc::new(shard));
        self.shards.sort_by(|left, right| {
            left.volume_id
                .cmp(&right.volume_id)
                .then(left.shard_id.cmp(&right.shard_id))
        });
        true
    }

    pub fn delete_shard(&mut self, shard_id: ShardId) -> Option<Arc<EcVolumeShard>> {
        let mut idx = None;
        for (i, shard) in self.shards.iter().enumerate() {
            if shard.shard_id == shard_id {
                idx = Some(i);
            }
        }
        idx.map(|idx| self.shards.remove(idx))
    }

    pub fn find_shard(&self, shard_id: ShardId) -> Option<Arc<EcVolumeShard>> {
        self.shards
            .iter()
            .find(|shard| shard.shard_id == shard_id)
            .cloned()
    }

    pub fn shards_len(&self) -> usize {
        self.shards.len()
    }

    pub fn locate_ec_shard_needle(
        &self,
        needle_id: NeedleId,
        version: Version,
    ) -> Result<(NeedleValue, Vec<Interval>)> {
        let needle_value = self.find_needle_from_ecx(needle_id)?;
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

    pub fn find_needle_from_ecx(&self, needle_id: NeedleId) -> Result<NeedleValue> {
        search_needle_from_sorted_index(&self.ecx_file, self.ecx_filesize, needle_id, None)
    }

    pub fn delete_needle_from_ecx(&mut self, needle_id: NeedleId) -> Result<()> {
        search_needle_from_sorted_index(
            &self.ecx_file,
            self.ecx_filesize,
            needle_id,
            Some(Box::new(mark_needle_deleted)),
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
                Some(Box::new(mark_needle_deleted)),
            )?;
        }

        fs::remove_file(&ecj_filename)?;
        Ok(())
    }

    pub fn filename(&self) -> String {
        ec_shard_filename(&self.collection, &self.dir, self.volume_id)
    }

    pub fn collection(&self) -> FastStr {
        self.collection.clone()
    }

    pub fn destroy(self) -> Result<()> {
        let filename = self.filename();
        for shard in self.shards.iter() {
            shard.destroy()?;
        }
        fs::remove_file(format!("{}.ecx", filename))?;
        fs::remove_file(format!("{}.ecj", filename))?;
        fs::remove_file(format!("{}.vif", filename))?;
        Ok(())
    }

    // heartbeat
    pub fn get_volume_ec_shard_info(&self) -> Vec<VolumeEcShardInformationMessage> {
        let mut infos = Vec::new();
        let mut pre_volume_id = u32::MAX as VolumeId;
        for shard in self.shards.iter() {
            if shard.volume_id != pre_volume_id {
                infos.push(VolumeEcShardInformationMessage {
                    id: shard.volume_id,
                    collection: shard.collection.to_string(),
                    ec_index_bits: 0,
                });
            }
            pre_volume_id = shard.volume_id;
            if let Some(info) = infos.last_mut() {
                info.ec_index_bits = add_shard_id(info.ec_index_bits, shard.shard_id);
            }
        }
        infos
    }
}

pub fn add_shard_id(ec_index_bits: u32, shard_id: ShardId) -> u32 {
    ec_index_bits | 1 << shard_id
}
