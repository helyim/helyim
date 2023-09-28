use std::{
    cmp::min,
    fs,
    io::{copy, ErrorKind, Read, Write},
    os::unix::fs::OpenOptionsExt,
};

use bytes::Buf;

use crate::{
    errors::Result,
    storage::{
        erasure_coding::{
            to_ext, ShardId, DATA_SHARDS_COUNT, ERASURE_CODING_LARGE_BLOCK_SIZE,
            ERASURE_CODING_SMALL_BLOCK_SIZE,
        },
        index_entry,
        needle::{actual_offset, actual_size, NEEDLE_ID_SIZE, NEEDLE_MAP_ENTRY_SIZE},
        types::{Offset, Size},
        version::Version,
        volume::{SuperBlock, DATA_FILE_SUFFIX, SUPER_BLOCK_SIZE},
        NeedleId, NeedleValue,
    },
    util::file_exists,
};

fn write_idx_file_from_ec_index(base_filename: &str) -> Result<()> {
    let mut ecx_file = fs::OpenOptions::new()
        .read(true)
        .mode(0o644)
        .open(format!("{}.ecx", base_filename))?;
    let mut idx_file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o644)
        .open(format!("{}.idx", base_filename))?;

    copy(&mut ecx_file, &mut idx_file)?;

    iterate_ecj_file(
        base_filename,
        Some(|needle_id| -> Result<()> {
            let buf = NeedleValue::deleted().as_bytes(needle_id);
            idx_file.write_all(&buf)?;
            Ok(())
        }),
    )
}

fn find_data_filesize(base_filename: &str) -> Result<u64> {
    let version = read_ec_volume_version(base_filename)?;
    let mut data_filesize = 0;
    iterate_ecx_file(
        base_filename,
        Some(|needle_id, offset, size: Size| -> Result<()> {
            if size.is_deleted() {
                return Ok(());
            }
            let entry_stop_offset = actual_offset(offset) + actual_size(size);
            if data_filesize < entry_stop_offset {
                data_filesize = entry_stop_offset;
            }
            Ok(())
        }),
    )?;
    Ok(data_filesize)
}

fn read_ec_volume_version(base_filename: &str) -> Result<Version> {
    let mut data_file = fs::OpenOptions::new()
        .read(true)
        .mode(0o644)
        .open(format!("{}.ec00", base_filename))?;
    let mut super_block = [0u8; SUPER_BLOCK_SIZE];
    data_file.read_exact(&mut super_block)?;
    let super_block = SuperBlock::parse(super_block)?;
    Ok(super_block.version)
}

fn iterate_ecx_file<F>(base_filename: &str, mut process_needle: Option<F>) -> Result<()>
where
    F: FnMut(NeedleId, Offset, Size) -> Result<()>,
{
    let mut ecx_file = fs::OpenOptions::new()
        .read(true)
        .mode(0o644)
        .open(format!("{}.ecx", base_filename))?;
    let mut buf = vec![0u8; NEEDLE_MAP_ENTRY_SIZE as usize];

    loop {
        if let Err(err) = ecx_file.read_exact(&mut buf) {
            return match err.kind() {
                ErrorKind::UnexpectedEof => Ok(()),
                _ => Err(err.into()),
            };
        }
        let (key, offset, size) = index_entry(&buf);
        if let Some(process_needle) = process_needle.as_mut() {
            process_needle(key, offset, size)?;
        }
    }
}

fn iterate_ecj_file<F>(base_filename: &str, mut process_needle: Option<F>) -> Result<()>
where
    F: FnMut(NeedleId) -> Result<()>,
{
    let ecj_filename = format!("{}.ecj", base_filename);
    if !file_exists(&ecj_filename)? {
        return Ok(());
    }
    let mut ecj_file = fs::OpenOptions::new()
        .read(true)
        .mode(0o644)
        .open(ecj_filename)?;
    let mut buf = [0u8; NEEDLE_ID_SIZE as usize];

    loop {
        if let Err(err) = ecj_file.read_exact(&mut buf) {
            return match err.kind() {
                ErrorKind::UnexpectedEof => Ok(()),
                _ => Err(err.into()),
            };
        }
        if let Some(process_needle) = process_needle.as_mut() {
            let needle_id = (&buf[..]).get_u64();
            process_needle(needle_id)?;
        }
    }
}

/// generates .dat from from .ec00 ~ .ec09 files
fn write_data_file(base_filename: &str, mut data_filesize: u64) -> Result<()> {
    let mut data_file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o644)
        .open(format!("{}.{}", base_filename, DATA_FILE_SUFFIX))?;

    let data_shards_count = DATA_SHARDS_COUNT as usize;
    let mut input_files = Vec::with_capacity(data_shards_count);
    for shard_id in 0..data_shards_count {
        let shard_filename = format!("{}.{}", base_filename, to_ext(shard_id as ShardId));
        let shard_file = fs::OpenOptions::new()
            .read(true)
            .mode(0o0)
            .open(shard_filename)?;
        input_files.push(shard_file);
    }

    while data_filesize >= DATA_SHARDS_COUNT as u64 * ERASURE_CODING_LARGE_BLOCK_SIZE {
        let mut large_block = vec![0u8; ERASURE_CODING_LARGE_BLOCK_SIZE as usize];
        for shard in input_files.iter_mut().take(data_shards_count) {
            shard.read_exact(&mut large_block)?;
            data_file.write_all(&large_block)?;
            data_filesize -= ERASURE_CODING_LARGE_BLOCK_SIZE;
        }
        assert!(data_filesize < DATA_SHARDS_COUNT as u64 * ERASURE_CODING_LARGE_BLOCK_SIZE);
    }

    while data_filesize > 0 {
        for shard in input_files.iter_mut().take(data_shards_count) {
            let read = min(data_filesize, ERASURE_CODING_SMALL_BLOCK_SIZE);
            let mut small_block = vec![0u8; read as usize];
            shard.read_exact(&mut small_block)?;
            data_file.write_all(&small_block)?;
            data_filesize -= read;
        }
        assert_eq!(data_filesize, 0);
    }

    Ok(())
}
