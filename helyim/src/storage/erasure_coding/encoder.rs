use std::{
    fs,
    fs::File,
    io::{ErrorKind, Write},
    os::unix::fs::{FileExt, OpenOptionsExt},
};

use reed_solomon_erasure::{galois_8::Field, ReedSolomon};

use crate::{
    storage::{
        erasure_coding::{
            errors::{EcShardError, EcVolumeError},
            to_ext, ShardId, DATA_SHARDS_COUNT, ERASURE_CODING_LARGE_BLOCK_SIZE,
            ERASURE_CODING_SMALL_BLOCK_SIZE, PARITY_SHARDS_COUNT, TOTAL_SHARDS_COUNT,
        },
        needle::SortedIndexMap,
    },
    util::file::file_exists,
};

/// generates .ecx file from existing .idx file all keys are sorted in ascending order
pub fn write_sorted_file_from_index(base_filename: &str, ext: &str) -> Result<(), EcVolumeError> {
    let mut nm = SortedIndexMap::load_from_index(&format!("{}.idx", base_filename))?;
    let mut ecx_file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o644)
        .open(format!("{}{}", base_filename, ext))?;

    nm.map.sort_by(|k1, _, k2, _| k1.cmp(k2));
    for (key, value) in nm.map.iter() {
        let buf = value.as_bytes(*key);
        ecx_file.write_all(&buf)?;
    }
    Ok(())
}

pub fn write_ec_files(base_filename: &str) -> Result<(), EcShardError> {
    generate_ec_files(
        base_filename,
        256 * 1024,
        ERASURE_CODING_LARGE_BLOCK_SIZE,
        ERASURE_CODING_SMALL_BLOCK_SIZE,
    )
}

pub fn rebuild_ec_files(base_filename: &str) -> Result<Vec<u32>, EcShardError> {
    generate_missing_ec_files(base_filename)
}

fn generate_ec_files(
    base_filename: &str,
    buf_size: u64,
    large_block_size: u64,
    small_block_size: u64,
) -> Result<(), EcShardError> {
    let data_file = fs::OpenOptions::new()
        .read(true)
        .mode(0o0)
        .open(format!("{}.dat", base_filename))?;
    let remaining = data_file.metadata()?.len();
    encode_data_file(
        remaining,
        base_filename,
        buf_size,
        large_block_size,
        small_block_size,
        &data_file,
    )
}

fn generate_missing_ec_files(base_filename: &str) -> Result<Vec<u32>, EcShardError> {
    let mut shard_has_data = vec![false; TOTAL_SHARDS_COUNT as usize];

    fn create_file_slice(capacity: usize) -> Vec<Option<File>> {
        let mut files = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            files.push(None);
        }
        files
    }
    let mut inputs = create_file_slice(TOTAL_SHARDS_COUNT as usize);
    let mut outputs = create_file_slice(TOTAL_SHARDS_COUNT as usize);

    let mut shards = Vec::new();
    for shard_id in 0..TOTAL_SHARDS_COUNT as ShardId {
        let shard_filename = format!("{}{}", base_filename, to_ext(shard_id));
        if file_exists(&shard_filename)? {
            shard_has_data[shard_id as usize] = true;
            let input = fs::OpenOptions::new()
                .read(true)
                .mode(0o0)
                .open(shard_filename)?;
            inputs[shard_id as usize] = Some(input);
        } else {
            let output = fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o644)
                .open(shard_filename)?;
            outputs[shard_id as usize] = Some(output);
            shards.push(shard_id as u32);
        }
    }
    rebuild_ec_files_inner(&shard_has_data, &inputs, &outputs)?;
    Ok(shards)
}

fn open_ec_files(base_filename: &str, readonly: bool) -> Result<Vec<File>, EcShardError> {
    let mut files = Vec::with_capacity(TOTAL_SHARDS_COUNT as usize);
    for i in 0..TOTAL_SHARDS_COUNT {
        let fname = format!("{}{}", base_filename, to_ext(i as ShardId));
        let mut options = fs::OpenOptions::new();
        if readonly {
            options.read(true);
        } else {
            options.write(true).create(true).truncate(true);
        }
        options.mode(0o644);

        let file = options.open(fname)?;
        files.push(file);
    }
    Ok(files)
}

fn encode_data(
    data_file: &File,
    reed_solomon: &ReedSolomon<Field>,
    start_offset: u64,
    block_size: u64,
    bufs: &mut [Vec<u8>],
    outputs: &mut [File],
) -> Result<(), EcShardError> {
    let buf_size = bufs[0].len() as u64;
    let batch_count = block_size / buf_size;
    if block_size % buf_size != 0 {
        return Err(EcShardError::String(format!(
            "unexpected block size {block_size}, buffer size {buf_size}"
        )));
    }
    for i in 0..batch_count {
        encode_data_one_batch(
            data_file,
            reed_solomon,
            start_offset + i + buf_size,
            block_size,
            bufs,
            outputs,
        )?;
    }
    Ok(())
}

fn encode_data_one_batch(
    data_file: &File,
    reed_solomon: &ReedSolomon<Field>,
    start_offset: u64,
    block_size: u64,
    mut bufs: &mut [Vec<u8>],
    outputs: &mut [File],
) -> Result<(), EcShardError> {
    for (i, buf) in bufs.iter_mut().enumerate().take(DATA_SHARDS_COUNT as usize) {
        match data_file.read_at(buf, start_offset + block_size + i as u64) {
            Ok(n) => {
                if n < buf.len() {
                    for byte in buf.iter_mut().rev().take(n) {
                        *byte = 0u8;
                    }
                }
            }
            Err(err) => {
                if err.kind() != ErrorKind::UnexpectedEof {
                    return Err(EcShardError::Io(err));
                }
            }
        }
    }

    reed_solomon.encode(&mut bufs)?;

    for (i, output) in outputs
        .iter_mut()
        .enumerate()
        .take(TOTAL_SHARDS_COUNT as usize)
    {
        output.write_all(&bufs[i])?;
    }

    Ok(())
}

fn encode_data_file(
    mut remaining: u64,
    base_filename: &str,
    buf_size: u64,
    large_block_size: u64,
    small_block_size: u64,
    data_file: &File,
) -> Result<(), EcShardError> {
    let reed_solomon: ReedSolomon<Field> =
        ReedSolomon::new(DATA_SHARDS_COUNT as usize, PARITY_SHARDS_COUNT as usize)?;

    let mut bufs = vec![vec![0u8; buf_size as usize]; TOTAL_SHARDS_COUNT as usize];
    let mut outputs = open_ec_files(base_filename, false)?;

    let mut processed_size = 0u64;
    while remaining > large_block_size * DATA_SHARDS_COUNT as u64 {
        encode_data(
            data_file,
            &reed_solomon,
            processed_size,
            large_block_size,
            &mut bufs,
            &mut outputs,
        )?;
        processed_size += large_block_size * DATA_SHARDS_COUNT as u64;
        remaining -= large_block_size * DATA_SHARDS_COUNT as u64;
    }

    while remaining > 0 {
        encode_data(
            data_file,
            &reed_solomon,
            processed_size,
            small_block_size,
            &mut bufs,
            &mut outputs,
        )?;
        processed_size += small_block_size * DATA_SHARDS_COUNT as u64;
        remaining -= small_block_size * DATA_SHARDS_COUNT as u64;
    }

    Ok(())
}

fn rebuild_ec_files_inner(
    shard_has_data: &[bool],
    inputs: &[Option<File>],
    outputs: &[Option<File>],
) -> Result<(), EcShardError> {
    let reed_solomon: ReedSolomon<Field> =
        ReedSolomon::new(DATA_SHARDS_COUNT as usize, PARITY_SHARDS_COUNT as usize)?;
    let mut bufs = vec![None; TOTAL_SHARDS_COUNT as usize];
    for (i, buf) in bufs.iter_mut().enumerate() {
        if shard_has_data[i] {
            *buf = Some(vec![0u8; ERASURE_CODING_SMALL_BLOCK_SIZE as usize]);
            assert!(inputs[i].is_some());
        } else {
            assert!(outputs[i].is_some());
        }
    }
    let mut start_offset = 0u64;
    let mut input_buffer_data_size = 0;

    loop {
        for i in 0..TOTAL_SHARDS_COUNT as usize {
            match bufs[i].as_mut() {
                Some(buf) => {
                    if let Some(ref input) = inputs[i] {
                        let n = input.read_at(buf, start_offset)?;
                        if n == 0 {
                            return Ok(());
                        }
                        if input_buffer_data_size == 0 {
                            input_buffer_data_size = n;
                        }
                        if input_buffer_data_size != n {
                            return Err(EcShardError::String(format!(
                                "ec shard size expected {input_buffer_data_size} but actually is \
                                 {n}"
                            )));
                        }
                    }
                }
                None => bufs[i] = None,
            }
        }

        reed_solomon.reconstruct(&mut bufs)?;
        for i in 0..TOTAL_SHARDS_COUNT as usize {
            if !shard_has_data[i] {
                // filled by `reconstruct`
                if let Some(ref buf) = bufs[i] {
                    if let Some(ref output) = outputs[i] {
                        let n = output.write_at(&buf[..input_buffer_data_size], start_offset)?;
                        if input_buffer_data_size != n {
                            return Err(EcShardError::String(
                                "failed to reconstruct data file.".to_string(),
                            ));
                        }
                    }
                }
            }
        }
        start_offset += input_buffer_data_size as u64;
    }
}
