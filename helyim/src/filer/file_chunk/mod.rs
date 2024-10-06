use std::{
    cmp::{max, min},
    collections::HashMap,
    hash::Hasher,
};

use fnv::FnvHasher;
use helyim_proto::filer::FileChunk;

pub fn total_size(chunks: &[FileChunk]) -> u64 {
    let mut size = 0;
    for chunk in chunks {
        size = max(chunk.offset as u64 + chunk.size, size);
    }
    size
}

pub fn etag(chunks: &[FileChunk]) -> String {
    if chunks.len() == 1 {
        return chunks[0].e_tag.clone();
    }
    let mut hasher = FnvHasher::default();
    for chunk in chunks {
        hasher.write(chunk.e_tag.as_bytes());
    }
    format!("{:x}", hasher.finish())
}

pub fn compact_file_chunks(chunks: &[FileChunk]) -> (Vec<FileChunk>, Vec<FileChunk>) {
    let visibles = non_overlapping_visible_intervals(chunks);
    let mut fids = HashMap::new();
    for interval in visibles {
        fids.insert(interval.file_id, true);
    }

    let mut compact = Vec::new();
    let mut garbage = Vec::new();
    for chunk in chunks {
        match fids.get(&chunk.fid) {
            Some(_) => compact.push(chunk.clone()),
            None => garbage.push(chunk.clone()),
        }
    }

    (compact, garbage)
}

pub fn find_unused_file_chunks(
    old_chunks: &[FileChunk],
    new_chunks: &[FileChunk],
) -> Vec<FileChunk> {
    let mut unused = vec![];
    let mut file_ids = HashMap::new();
    for interval in new_chunks {
        file_ids.insert(&interval.fid, true);
    }
    for chunk in old_chunks {
        if file_ids.contains_key(&chunk.fid) {
            unused.push(chunk.clone());
        }
    }
    unused
}

/// Find non-overlapping visible intervals visible interval
/// map to one file chunk
#[derive(Clone)]
struct VisibleInterval {
    start: i64,
    stop: i64,
    modified_time: i64,
    file_id: String,
    is_full_chunk: bool,
}

impl VisibleInterval {
    fn new(
        start: i64,
        stop: i64,
        modified_time: i64,
        file_id: String,
        is_full_chunk: bool,
    ) -> Self {
        Self {
            start,
            stop,
            modified_time,
            file_id,
            is_full_chunk,
        }
    }
}

fn non_overlapping_visible_intervals(chunks: &[FileChunk]) -> Vec<VisibleInterval> {
    // chunks.sort_by(|i, j| -> Ordering { j.mtime.cmp(&i.mtime) });
    let mut visibles = Vec::new();
    let mut new_visibles = Vec::new();
    for chunk in chunks {
        if let Some(v) = merge_into_visibles(&mut visibles, &mut new_visibles, chunk) {
            visibles = v.clone();
        }
        new_visibles.clear();
    }
    visibles
}

fn merge_into_visibles<'a>(
    visibles: &'a mut Vec<VisibleInterval>,
    new_visibles: &'a mut Vec<VisibleInterval>,
    chunk: &FileChunk,
) -> Option<&'a Vec<VisibleInterval>> {
    let new_v = VisibleInterval::new(
        chunk.offset,
        chunk.offset + chunk.size as i64,
        chunk.mtime,
        chunk.fid.clone(),
        true,
    );

    match visibles.last() {
        Some(last) => {
            if last.stop <= chunk.offset {
                visibles.push(new_v);
                return Some(visibles);
            }
        }
        None => {
            visibles.push(new_v);
            return Some(visibles);
        }
    }

    for v in visibles {
        if v.start < chunk.offset && chunk.offset < v.stop {
            new_visibles.push(VisibleInterval::new(
                v.start,
                chunk.offset,
                v.modified_time,
                v.file_id.clone(),
                false,
            ));
        }

        let chunk_stop = chunk.offset + chunk.size as i64;
        if v.start < chunk_stop && chunk_stop < v.stop {
            new_visibles.push(VisibleInterval::new(
                chunk_stop,
                v.stop,
                v.modified_time,
                v.file_id.clone(),
                false,
            ));
        }

        if chunk_stop <= v.start || v.stop <= chunk.offset {
            new_visibles.push(v.clone());
        }
    }

    new_visibles.push(new_v.clone());

    for i in (0..new_visibles.len()).rev() {
        if i > 0 && new_v.start < new_visibles[i - 1].start {
            new_visibles[i] = new_visibles[i - 1].clone();
        } else {
            new_visibles[i] = new_v.clone();
            break;
        }
    }

    Some(new_visibles)
}

pub struct ChunkView {
    pub fid: String,
    pub offset: i64,
    pub size: u32,
    pub logic_offset: i64,
    pub is_full_chunk: bool,
}

impl ChunkView {
    pub fn view_from_chunks(chunks: &[FileChunk], mut offset: i64, size: u64) -> Vec<ChunkView> {
        let visibles = non_overlapping_visible_intervals(chunks);
        let stop = offset + size as i64;

        let mut views = Vec::new();
        for chunk in visibles {
            if chunk.start <= offset && offset < chunk.stop && offset < stop {
                let is_full_chunk =
                    chunk.is_full_chunk && chunk.start == offset && chunk.stop <= stop;
                let min = min(chunk.stop, stop);
                views.push(ChunkView {
                    fid: chunk.file_id.clone(),
                    offset: offset - chunk.start,
                    size: (min - offset) as u32,
                    logic_offset: offset,
                    is_full_chunk,
                });
                offset = min;
            }
        }

        views
    }
}
