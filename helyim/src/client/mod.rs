mod location;

use std::{ops::Deref, sync::Arc, time::Duration};

use arc_swap::{ArcSwap, Guard};
use async_stream::stream;
use faststr::FastStr;
use helyim_proto::directory::KeepConnectedRequest;
use nom::error::Error as NomError;
use tokio_stream::StreamExt;
use tonic::Status;
use tracing::{error, info, warn};

use crate::{
    client::location::{Location, LocationMap},
    storage::{VolumeError, VolumeId},
    util::grpc::helyim_client,
};

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
            current_master: ArcSwap::new(Arc::new(FastStr::empty())),
            masters,
            locations: LocationMap::default(),
        }
    }
}

impl MasterClient {
    pub async fn current_master(&self) -> Guard<Arc<FastStr>> {
        self.current_master.load()
    }

    pub async fn keep_connected_to_master(&self) {
        let mut interval = tokio::time::interval(Duration::from_secs(3));
        loop {
            interval.tick().await;
            let _ = self.try_all_masters().await;
        }
    }

    async fn try_all_masters(&self) -> Result<(), ClientError> {
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
    #[error("Volume id {0} not found")]
    VolumeIdNotFound(VolumeId),

    #[error("Volume error: {0}")]
    Volume(#[from] VolumeError),

    #[error("Keep connected to {0} error: {1}")]
    KeepConnected(FastStr, Status),
}

impl From<nom::Err<NomError<&str>>> for ClientError {
    fn from(value: nom::Err<NomError<&str>>) -> Self {
        ClientError::String(value.to_string())
    }
}
