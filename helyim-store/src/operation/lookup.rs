use std::time::Duration;

use helyim_client::helyim_client;
use helyim_common::{parser::parse_vid_fid, types::VolumeId};
use helyim_proto::directory::{
    LookupVolumeRequest, LookupVolumeResponse, lookup_volume_response::VolumeIdLocation,
};
use helyim_topology::TopologyError;
use moka::sync::{Cache, CacheBuilder};
use tonic::Status;

pub struct Looker {
    volumes: Cache<VolumeId, VolumeIdLocation>,
}

impl Default for Looker {
    fn default() -> Self {
        Self::new()
    }
}

impl Looker {
    pub fn new() -> Looker {
        Looker {
            volumes: CacheBuilder::new(u64::MAX)
                .time_to_live(Duration::from_secs(600))
                .build(),
        }
    }

    async fn do_lookup(
        &self,
        vids: &[VolumeId],
        master: &str,
    ) -> Result<LookupVolumeResponse, Status> {
        let request = LookupVolumeRequest {
            volume_or_file_ids: vids.iter().map(|vid| vid.to_string()).collect(),
            collection: String::default(),
        };

        let client = helyim_client(master)?;
        let response = client.lookup_volume(request).await?;
        Ok(response.into_inner())
    }

    pub async fn lookup(
        &self,
        vids: Vec<VolumeId>,
        master: &str,
    ) -> Result<Vec<VolumeIdLocation>, TopologyError> {
        let mut volume_locations = Vec::with_capacity(vids.len());
        let mut volume_ids = vec![];
        for vid in vids {
            match self.volumes.get(&vid) {
                Some(value) => volume_locations.push(value),
                None => volume_ids.push(vid),
            }
        }

        match self.do_lookup(&volume_ids, master).await {
            Ok(lookup) => {
                for location in lookup.volume_id_locations {
                    volume_locations.push(location.clone());
                    if !location.error.is_empty() {
                        let (_, (vid, _fid)) = parse_vid_fid(&location.volume_or_file_id)?;
                        let vid = vid.parse()?;
                        self.volumes.insert(vid, location);
                    }
                }
                Ok(volume_locations)
            }
            Err(err) => Err(TopologyError::Box(Box::new(err))),
        }
    }
}
