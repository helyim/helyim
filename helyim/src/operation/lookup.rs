use std::time::Duration;

use ginepro::LoadBalancedChannel;
use helyim_proto::{
    helyim_client::HelyimClient, lookup_volume_response::VolumeLocation, LookupVolumeRequest,
    LookupVolumeResponse,
};
use moka::sync::{Cache, CacheBuilder};

use crate::{errors::Result, storage::VolumeId};

pub struct Looker {
    volumes: Cache<VolumeId, VolumeLocation>,
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
        client: &mut HelyimClient<LoadBalancedChannel>,
    ) -> Result<LookupVolumeResponse> {
        let request = LookupVolumeRequest {
            volumes: vids.iter().map(|vid| vid.to_string()).collect(),
            collection: String::default(),
        };
        let response = client.lookup_volume(request).await?;
        Ok(response.into_inner())
    }

    pub async fn lookup(
        &self,
        vids: Vec<VolumeId>,
        client: &mut HelyimClient<LoadBalancedChannel>,
    ) -> Result<Vec<VolumeLocation>> {
        let mut volume_locations = Vec::with_capacity(vids.len());
        let mut volume_ids = vec![];
        for vid in vids {
            match self.volumes.get(&vid) {
                Some(value) => volume_locations.push(value),
                None => volume_ids.push(vid),
            }
        }

        match self.do_lookup(&volume_ids, client).await {
            Ok(lookup) => {
                for location in lookup.volume_locations {
                    volume_locations.push(location.clone());
                    if !location.error.is_empty() {
                        self.volumes.insert(location.volume_id, location);
                    }
                }
                Ok(volume_locations)
            }
            Err(err) => Err(err),
        }
    }
}
