use std::{cmp::max, hash::{DefaultHasher, Hash, Hasher}};

use faststr::FastStr;
use helyim_proto::filer::FileChunk;

use crate::storage::FileId;

pub fn total_size(chunks: &[FileChunk]) -> u64 {
    let mut size = 0;
    for chunk in chunks {
        size = max(chunk.offset as u64 + chunk.size, size);
    }
    size
}

// should use 32-bit FNV-1a hash?
pub fn etag(chunks: &[FileChunk]) -> FastStr {
    if chunks.len() == 1 {
        return chunks[0].e_tag.clone().into();
    }

    let mut hasher = DefaultHasher::new();
    chunks.iter().for_each(|chunk| {
        chunk.e_tag.hash(&mut hasher);
    });

    format!("{}", hasher.finish()).into()
}

pub fn view_from_chunks(chunks: &mut [FileChunk], offset: i64, size: u64) -> Vec<ChunkView> {
    let visibles = non_overlapping_visible_intervals(chunks);
    view_from_visible_intervals(visibles, offset, size)
}

fn view_from_visible_intervals(visibles: Vec<VisibleInterval>, offset: i64, size: u64) -> Vec<ChunkView> {
    let stop = offset + size as i64;
    let mut views = Vec::new();
    visibles.into_iter().for_each(|visible| {
        if visible.start <= offset && offset < visible.stop && offset < stop {
            let is_full_chunk = visible.is_full_chunk && visible.start == offset && visible.stop == stop;
            views.push(ChunkView {
                file_id: visible.file_id,
                offset: offset - visible.start,
                size: (visible.stop.min(stop) - offset) as u64,
                logic_offset: offset,
                is_full_chunk,
            });
        }
    });
    
    views
}

fn non_overlapping_visible_intervals(chunks: &mut [FileChunk]) -> Vec<VisibleInterval> {
    chunks.sort_by(|a: &FileChunk, b| { a.modified_ts_ns.cmp(&b.modified_ts_ns) });

    let mut visibles = Vec::new();
    chunks.iter().for_each(|chunk| {
        visibles = merge_into_visibles(&mut visibles, chunk);
    });

    visibles
}

#[derive(Clone)]
pub struct ChunkView {
	pub file_id: FastStr,
	pub offset: i64,
	pub size: u64,
	pub logic_offset: i64,
	pub is_full_chunk: bool,
}

#[derive(Clone)]
pub struct VisibleInterval {
	start: i64,
	stop: i64,
	modified_time: i64,
	file_id: FastStr,
	is_full_chunk: bool,
}

fn merge_into_visibles(visibles: &mut Vec<VisibleInterval>, chunk: &FileChunk) -> Vec<VisibleInterval> {
    let new_visible = VisibleInterval {
        start: chunk.offset,
        stop: chunk.offset+ chunk.size as i64,
        modified_time: chunk.modified_ts_ns,
        file_id: format!("{}", FileId::from(&chunk.fid.clone().unwrap())).into(),
        is_full_chunk: true,
    };

    if visibles.is_empty() || visibles.last().unwrap().stop < chunk.offset {
        visibles.push(new_visible);
        return visibles.clone();
    }

    let mut new_visibles = Vec::new();
    for visible in visibles {
        if visible.start < chunk.offset && chunk.offset < visible.stop {
            new_visibles.push(VisibleInterval {
                start: visible.start,
                stop: chunk.offset,
                modified_time: visible.modified_time,
                file_id: visible.file_id.clone(),
                is_full_chunk: false,
            });
        }

        let chunk_stop = chunk.offset+ chunk.size as i64;
        if visible.start < chunk_stop && chunk_stop < visible.stop {
            new_visibles.push(VisibleInterval {
                start: chunk_stop,
                stop: visible.stop,
                modified_time: visible.modified_time,
                file_id: visible.file_id.clone(),
                is_full_chunk: false,
            });
        }

        if chunk_stop <= visible.start || visible.stop < chunk.offset {
            new_visibles.push(visible.clone());
        }
    }

    new_visibles.push(new_visible.clone());

    for i in new_visibles.len()..0 {
        if i > 0 && new_visible.start < new_visibles[i-1].start {
            new_visibles[i] = new_visibles[i - 1].clone();
        } else {
            new_visibles[i] = new_visible;
            break;
        }
    }

    new_visibles
}
