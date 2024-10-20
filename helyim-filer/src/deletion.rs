use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use faststr::FastStr;
use futures::channel::mpsc::UnboundedReceiver;
use futures_util::StreamExt;
use helyim_proto::filer::FileChunk;
use tracing::info;

use crate::{
    entry::Entry,
    filer::Filer,
    operation::{Location, Lookup},
    FilerError,
};

impl Filer {
    pub fn delete_chunks(&self, chunks: &[FileChunk]) -> Result<(), FilerError> {
        for chunk in chunks {
            self.delete_file_id_tx.unbounded_send(chunk.fid.clone())?;
        }

        Ok(())
    }

    pub fn delete_chunks_if_not_new(
        &self,
        old_entry: Option<&Entry>,
        new_entry: Option<&Entry>,
    ) -> Result<(), FilerError> {
        let old_entry = match old_entry {
            Some(entry) => entry,
            None => return Ok(()),
        };

        let new_entry = match new_entry {
            Some(entry) => entry,
            None => {
                self.delete_chunks(&old_entry.chunks)?;
                return Ok(());
            }
        };

        let new_fids: HashSet<_> = new_entry.chunks.iter().map(|chunk| &chunk.fid).collect();
        let to_delete: Vec<FileChunk> = old_entry
            .chunks
            .iter()
            .filter(|old_chunk| !new_fids.contains(&old_chunk.fid))
            .cloned()
            .collect();
        self.delete_chunks(&to_delete)
    }

    fn lookup(&self, vids: Vec<FastStr>) -> HashMap<FastStr, Lookup> {
        let mut map = HashMap::new();
        for vid in vids {
            let mut locations = vec![];
            if let Some(locs) = self.master_client.get_locations(&vid) {
                for loc in locs.iter() {
                    locations.push(Location {
                        url: loc.url.clone(),
                        public_url: loc.public_url.clone(),
                    });
                }

                let lookup = Lookup::ok(&vid, locations);
                map.insert(vid, lookup);
            }
        }
        map
    }
}

pub async fn loop_processing_deletion(mut rx: UnboundedReceiver<String>) {
    let mut interval = tokio::time::interval(Duration::from_secs(5));

    let mut file_ids = vec![];
    loop {
        tokio::select! {
            _ = interval.tick() => {
                if !file_ids.is_empty() {
                    info!("deleting file ids[len={}]", file_ids.len());
                    // TODO: delete files with lookup volume id
                }
            }
            file_id = rx.next() => {
                file_ids.push(file_id);
                if file_ids.len() > 100_000 {
                    info!("deleting file ids[len={}]", file_ids.len());
                    // TODO: delete files with lookup volume id
                }
            }
        }
        file_ids.clear();
    }
}
