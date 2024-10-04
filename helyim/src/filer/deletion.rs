use std::{collections::HashMap, time::Duration};

use faststr::FastStr;
use futures::{channel::mpsc::UnboundedReceiver, StreamExt};
use helyim_proto::filer::{FileChunk, FileId};
use tracing::info;

use crate::{
    client::{ClientError, MasterClient},
    filer::{entry::Entry, Filer, FilerError},
    operation::lookup::{Location, Lookup},
};

impl Filer {
    pub fn delete_chunks(&self, chunks: &[FileChunk]) -> Result<(), FilerError> {
        for chunk in chunks {
            self.delete_file_id_tx.unbounded_send(chunk.fid)?;
        }

        Ok(())
    }

    pub fn delete_chunks_if_not_new(
        &self,
        old_entry: Option<&Entry>,
        new_entry: Option<&Entry>,
    ) -> Result<(), FilerError> {
        if old_entry.is_none() {
            return Ok(());
        }
        let old_entry = old_entry.unwrap();

        if new_entry.is_none() {
            self.delete_chunks(&old_entry.chunks)?;
        }

        let mut to_delete = Vec::new();
        for old_chunk in old_entry.chunks.iter() {
            let mut found = false;
            if let Some(new_entry) = new_entry {
                for new_chunk in new_entry.chunks.iter() {
                    if old_chunk.fid == new_chunk.fid {
                        found = true;
                        break;
                    }
                }
            }
            if !found {
                to_delete.push(old_chunk.clone());
            }
        }
        self.delete_chunks(&to_delete)
    }

    #[allow(dead_code)] // todo: remove it
    fn lookup(&self, vids: Vec<FastStr>) -> HashMap<FastStr, Lookup> {
        let mut map = HashMap::new();
        for vid in vids {
            let mut locations = vec![];
            if let Some(locs) = self.master_client.get_locations(&vid) {
                for loc in locs.iter() {
                    locations.push(Location {
                        url: loc.url.to_string(),
                        public_url: loc.public_url.to_string(),
                    });
                }

                let lookup = Lookup::ok(&vid, locations);
                map.insert(vid, lookup);
            }
        }
        map
    }
}

#[allow(dead_code)]
fn lookup_by_master_client(
    master_client: &MasterClient,
    vids: Vec<String>,
) -> Result<HashMap<String, Lookup>, ClientError> {
    let mut map = HashMap::with_capacity(vids.len());
    for vid in vids {
        let mut locations = Vec::new();
        if let Some(locs) = master_client.get_locations(&vid) {
            for loc in locs.iter() {
                locations.push(Location {
                    url: loc.url.to_string(),
                    public_url: loc.public_url.to_string(),
                });
            }
            map.insert(vid.clone(), Lookup::ok(vid, locations));
        }
    }
    Ok(map)
}

pub async fn loop_processing_deletion(mut rx: UnboundedReceiver<Option<FileId>>) {
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
            Some(file_id) = rx.next() => {
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
