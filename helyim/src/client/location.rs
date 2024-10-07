use std::sync::Arc;

use dashmap::{mapref::one::Ref, DashMap};
use faststr::FastStr;
use rand::Rng;

use crate::{client::ClientError, storage::VolumeId, util::parser::parse_vid_fid};

#[derive(Clone)]
pub struct Location {
    pub url: FastStr,
    pub public_url: FastStr,
}

#[derive(Default)]
pub struct LocationMap {
    locations: Arc<DashMap<VolumeId, Vec<Location>>>,
}

impl LocationMap {
    pub fn lookup_volume_server_url(&self, vid: &str) -> Result<FastStr, ClientError> {
        let vid = vid
            .parse()
            .map_err(|_| ClientError::UnknownVolumeId(vid.to_string()))?;
        let locations = self.get_locations_by_vid(vid);
        if let Some(locations) = locations {
            if !locations.is_empty() {
                let idx = rand::thread_rng().gen_range(0..locations.len());
                let url = locations[idx].url.clone();
                return Ok(url);
            }
        }
        Err(ClientError::VolumeIdNotFound(vid))
    }

    pub fn lookup_file_id(&self, file_id: &str) -> Result<String, ClientError> {
        let (_, (vid, file_id)) = parse_vid_fid(file_id)?;
        let url = self.lookup_volume_server_url(vid)?;
        Ok(format!("http://{url}/{file_id}"))
    }

    pub fn lookup_volume_server(&self, file_id: &str) -> Result<FastStr, ClientError> {
        let (_, (vid, _file_id)) = parse_vid_fid(file_id)?;
        let url = self.lookup_volume_server_url(vid)?;
        Ok(url)
    }

    pub fn get_locations(&self, vid: &str) -> Option<Ref<VolumeId, Vec<Location>>> {
        match vid.parse() {
            Ok(vid) => self.get_locations_by_vid(vid),
            Err(_err) => None,
        }
    }

    pub fn get_locations_by_vid(&self, vid: VolumeId) -> Option<Ref<VolumeId, Vec<Location>>> {
        self.locations.get(&vid)
    }

    pub fn add_location(&self, vid: VolumeId, location: Location) {
        let mut locations = self.locations.entry(vid).or_default();
        if locations.is_empty() {
            locations.push(location);
            return;
        }
        for loc in locations.iter() {
            if loc.url == location.url {
                return;
            }
        }
        locations.push(location);
    }

    pub fn delete_location(&self, vid: VolumeId, location: Location) {
        let mut locations = self.locations.entry(vid).or_default();
        if locations.is_empty() {
            return;
        }
        locations.retain(|loc| loc.url != location.url);
    }

    pub fn clear(&self) {
        self.locations.clear();
    }
}
