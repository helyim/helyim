use std::{
    fs,
    fs::File,
    io::Read,
    os::unix::fs::{FileExt, OpenOptionsExt},
};

use bytes::{Buf, BufMut};

use crate::{
    storage::{
        needle::{
            NEEDLE_ENTRY_SIZE, NEEDLE_HEADER_SIZE, NEEDLE_ID_SIZE, OFFSET_SIZE, SIZE_SIZE,
            TOMBSTONE_FILE_SIZE,
        },
        read_index_entry, NeedleError, NeedleId, NeedleValue,
    },
    util::file::file_exists,
};

mod decoder;
pub use decoder::{find_data_filesize, write_data_file, write_index_file_from_ec_index};

mod encoder;
pub use encoder::{rebuild_ec_files, write_ec_files, write_sorted_file_from_index};

mod disk_location;

mod errors;
pub use errors::{EcShardError, EcVolumeError};

mod locate;
pub use locate::Interval;

mod shard;
pub use shard::{ec_shard_base_filename, ec_shard_filename, EcVolumeShard};

mod store;

mod volume;
pub use volume::{add_shard_id, EcVolumeRef};

mod volume_info;
pub use volume_info::{EcVolumeInfo, ShardBits};

pub type ShardId = u8;

pub const DATA_SHARDS_COUNT: u32 = 10;
pub const PARITY_SHARDS_COUNT: u32 = 4;
pub const TOTAL_SHARDS_COUNT: u32 = DATA_SHARDS_COUNT + PARITY_SHARDS_COUNT;
pub const ERASURE_CODING_LARGE_BLOCK_SIZE: u64 = 1024 * 1024 * 1024;
pub const ERASURE_CODING_SMALL_BLOCK_SIZE: u64 = 1024 * 1024;

type ProcessNeedleFn = Box<dyn FnMut(&File, u64) -> Result<(), NeedleError>>;

fn search_needle_from_sorted_index(
    ecx_file: &File,
    ecx_filesize: u64,
    needle_id: NeedleId,
    process_needle: Option<ProcessNeedleFn>,
) -> Result<NeedleValue, NeedleError> {
    let mut buf = [0u8; NEEDLE_ENTRY_SIZE as usize];
    let (mut low, mut high) = (0u64, ecx_filesize / NEEDLE_ENTRY_SIZE as u64);
    while low < high {
        let middle = (low + high) / 2;
        ecx_file.read_at(&mut buf, middle * NEEDLE_ENTRY_SIZE as u64)?;
        let (key, offset, size) = read_index_entry(&buf);
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
    Err(NeedleError::NotFound(needle_id))
}

pub fn to_ext(ec_idx: ShardId) -> String {
    format!(".ec{:02}", ec_idx)
}

fn mark_needle_deleted(file: &File, offset: u64) -> Result<(), NeedleError> {
    let mut buf = vec![0u8; SIZE_SIZE as usize];
    buf.put_i32(TOMBSTONE_FILE_SIZE);
    file.write_all_at(&buf, offset + NEEDLE_ID_SIZE as u64 + OFFSET_SIZE as u64)?;
    Ok(())
}

pub fn rebuild_ecx_file(base_filename: &str) -> Result<(), NeedleError> {
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
            Some(Box::new(mark_needle_deleted)),
        ) {
            if !matches!(err, NeedleError::NotFound(_)) {
                return Err(err);
            }
        }
    }

    fs::remove_file(ecj_filename)?;

    Ok(())
}
