use std::{num::ParseIntError, time::Duration};

use faststr::FastStr;
use helyim_common::types::VolumeId;
use helyim_proto::directory::{
    lookup_volume_response::VolumeIdLocation, LookupVolumeRequest, LookupVolumeResponse,
};
use moka::sync::{Cache, CacheBuilder};
use serde::{Deserialize, Serialize};
use tonic::Status;

use crate::{
    errors::Result,
    util::{grpc::helyim_client, parser::parse_vid_fid},
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LookupRequest {
    pub volume_id: FastStr,
    pub collection: Option<FastStr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Location {
    pub url: FastStr,
    pub public_url: FastStr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Lookup {
    pub volume_id: FastStr,
    pub locations: Vec<Location>,
    pub error: FastStr,
}

impl Lookup {
    pub fn ok<S: AsRef<str>>(vid: S, locations: Vec<Location>) -> Self {
        Self {
            volume_id: FastStr::new(vid),
            locations,
            error: FastStr::empty(),
        }
    }

    pub fn error<S: AsRef<str>>(error: S) -> Self {
        Self {
            volume_id: FastStr::empty(),
            locations: vec![],
            error: FastStr::new(error),
        }
    }
}

pub struct Looker {
    volumes: Cache<VolumeId, VolumeIdLocation>,
}

impl Looker {
    pub fn new() -> Looker {
        Looker {
            volumes: CacheBuilder::new(u64::MAX)
                .time_to_live(Duration::from_secs(600))
                .build(),
        }
    }

    async fn do_lookup(&self, vids: &[VolumeId], master: &str) -> Result<LookupVolumeResponse> {
        let request = LookupVolumeRequest {
            volume_or_file_ids: vids.iter().map(|vid| vid.to_string()).collect(),
            collection: String::default(),
        };

        let client = helyim_client(master)?;
        let response = client.lookup_volume(request).await?;
        Ok(response.into_inner())
    }

    pub async fn lookup(&self, vids: Vec<VolumeId>, master: &str) -> Result<Vec<VolumeIdLocation>> {
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
                        let vid = vid.parse().map_err(|err: ParseIntError| {
                            Status::invalid_argument(format!("parse volume id error: {err}"))
                        })?;
                        self.volumes.insert(vid, location);
                    }
                }
                Ok(volume_locations)
            }
            Err(err) => Err(err),
        }
    }
}
