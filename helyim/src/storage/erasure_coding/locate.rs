use crate::storage::erasure_coding::{ShardId, DATA_SHARDS_COUNT};

pub struct Interval {
    block_index: u64,
    inner_block_offset: u64,
    size: u64,
    is_large_block: bool,
    large_block_rows: u64,
}

impl Interval {
    pub fn shard_id(&self) -> ShardId {
        (self.block_index % DATA_SHARDS_COUNT as u64) as ShardId
    }

    pub fn offset(&self, large_block_size: u64, small_block_size: u64) -> u64 {
        let mut ec_file_offset = self.inner_block_offset;
        let row_index = self.block_index / DATA_SHARDS_COUNT as u64;
        if self.is_large_block {
            ec_file_offset += row_index * large_block_size;
        } else {
            ec_file_offset +=
                self.large_block_rows * large_block_size + row_index * small_block_size;
        }
        ec_file_offset
    }
}

pub fn locate_data(
    large_block_len: u64,
    small_block_len: u64,
    data_size: u64,
    offset: u64,
    mut size: u64,
) -> Vec<Interval> {
    let (mut block_index, mut is_large_block, mut inner_block_offset) =
        locate_offset(large_block_len, small_block_len, data_size, offset);

    let large_block_rows = (data_size + small_block_len * DATA_SHARDS_COUNT as u64)
        / (large_block_len * DATA_SHARDS_COUNT as u64);

    let mut intervals = Vec::new();
    while size > 0 {
        let mut interval = Interval {
            block_index,
            inner_block_offset,
            is_large_block,
            large_block_rows,
            size: 0,
        };

        let mut block_remaining = large_block_len - inner_block_offset;
        if !is_large_block {
            block_remaining = small_block_len - inner_block_offset;
        }

        if size <= block_remaining {
            interval.size = size;
            intervals.push(interval);
            return intervals;
        }

        interval.size = block_remaining;
        intervals.push(interval);
        size -= block_remaining;
        block_index += 1;
        if is_large_block && block_index == large_block_rows * DATA_SHARDS_COUNT as u64 {
            is_large_block = false;
            block_index = 0;
        }
        inner_block_offset = 0;
    }
    intervals
}

fn locate_offset(
    large_block_len: u64,
    small_block_len: u64,
    data_size: u64,
    offset: u64,
) -> (u64, bool, u64) {
    let large_row_size = large_block_len * DATA_SHARDS_COUNT as u64;
    let large_block_rows = data_size / (large_block_len * DATA_SHARDS_COUNT as u64);

    if offset < large_block_rows * large_row_size {
        let (block_idx, inner_block_offset) = locate_offset_within_blocks(large_block_len, offset);
        return (block_idx, true, inner_block_offset);
    }

    let offset = offset - large_block_rows * large_row_size;
    let (block_idx, inner_block_offset) = locate_offset_within_blocks(small_block_len, offset);
    (block_idx, false, inner_block_offset)
}

fn locate_offset_within_blocks(block_len: u64, offset: u64) -> (u64, u64) {
    let block_idx = offset / block_len;
    let inner_block_offset = offset % block_len;
    (block_idx, inner_block_offset)
}
