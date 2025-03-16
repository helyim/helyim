use std::{ops::Deref, sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use async_stream::stream;
use faststr::FastStr;
use helyim_common::{parser::ParseError, types::VolumeId};
use helyim_proto::directory::KeepConnectedRequest;
use tokio_stream::StreamExt;
use tonic::Status;
use tracing::{debug, error, info, warn};

use crate::location::{Location, LocationMap};

mod location;
mod pool;
pub use pool::{helyim_client, volume_server_client};

pub struct MasterClient {
    name: FastStr,
    current_master: ArcSwap<FastStr>,
    masters: Vec<FastStr>,
    locations: LocationMap,
}

impl MasterClient {
    pub fn new(name: &str, masters: Vec<FastStr>) -> Self {
        Self {
            name: FastStr::new(name),
            current_master: ArcSwap::default(),
            masters,
            locations: LocationMap::default(),
        }
    }
}

impl MasterClient {
    pub fn current_master(&self) -> String {
        self.current_master.load().to_string()
    }

    pub async fn keep_connected_to_master(&self) {
        let mut interval = tokio::time::interval(Duration::from_secs(3));
        loop {
            interval.tick().await;
            let _ = self.try_all_masters().await;
        }
    }

    pub async fn try_all_masters(&self) -> Result<(), ClientError> {
        for master in self.masters.iter() {
            let mut next_hinted_leader = self.try_connect_to_master(master).await?;
            while !next_hinted_leader.is_empty() {
                next_hinted_leader = self.try_connect_to_master(&next_hinted_leader).await?;
            }
            self.current_master.store(Arc::new(FastStr::empty()));
            self.locations.clear();
        }

        Ok(())
    }

    async fn try_connect_to_master(&self, master: &str) -> Result<FastStr, ClientError> {
        let master = FastStr::new(master);

        let mut interval = tokio::time::interval(Duration::from_secs(1));

        let client_name = self.name.clone();
        let request_stream = stream! {
            loop {
                yield KeepConnectedRequest {
                    name: client_name.to_string(),
                };
                interval.tick().await;
            }
        };

        debug!("connecting to master: {master}");
        let client = helyim_client(&master)?;
        let mut next_hinted_leader = FastStr::empty();
        match client.keep_connected(request_stream).await {
            Ok(response) => {
                self.current_master.store(Arc::new(master.clone()));
                info!("current master is {}", master);

                let mut stream = response.into_inner();
                while let Some(location_ret) = stream.next().await {
                    match location_ret {
                        Ok(location) => {
                            if let Some(leader) = location.leader {
                                next_hinted_leader = FastStr::new(leader);
                                return Ok(next_hinted_leader);
                            }

                            let loc = Location {
                                url: FastStr::new(location.url),
                                public_url: FastStr::new(location.public_url),
                            };

                            for vid in location.new_vids {
                                self.locations.add_location(vid, loc.clone());
                            }

                            for vid in location.deleted_vids {
                                self.locations.delete_location(vid, loc.clone());
                            }
                        }
                        Err(err) => {
                            error!("{} failed to received from {master}", self.name);
                            return Err(ClientError::KeepConnected(master, err));
                        }
                    }
                }
                Ok(next_hinted_leader)
            }
            Err(status) => {
                warn!("keep connected to {master} error: {}", status.message());
                Err(ClientError::KeepConnected(master, status))
            }
        }
    }
}

impl Deref for MasterClient {
    type Target = LocationMap;

    fn deref(&self) -> &Self::Target {
        &self.locations
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ClientError {
    #[error("{0}")]
    Box(Box<dyn std::error::Error>),
    #[error("{0}")]
    String(String),
    #[error("Unknown volume id: {0}")]
    UnknownVolumeId(String),
    #[error("Volume {0} not found")]
    VolumeNotFound(VolumeId),

    #[error("Keep connected to {0} error: {1}")]
    KeepConnected(FastStr, Status),

    #[error("Tonic error: {0}")]
    Tonic(#[from] Status),

    #[error("Parse error: {0}")]
    Parse(#[from] ParseError),
}
