use std::cmp::max;

use helyim_proto::filer::FileChunk;

pub fn total_size(chunks: &[FileChunk]) -> u64 {
    let mut size = 0;
    for chunk in chunks {
        size = max(chunk.offset as u64 + chunk.size, size);
    }
    size
}
