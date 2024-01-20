mod location;

use std::{ops::Deref, time::Duration};

use async_stream::stream;
use faststr::FastStr;
use helyim_proto::ClientListenRequest;
use nom::error::Error as NomError;
use tokio_stream::StreamExt;
use tonic::Status;
use tracing::{error, info};

use crate::{
    client::location::{Location, LocationMap},
    storage::{VolumeError, VolumeId},
    util::grpc::helyim_client,
};

#[derive(Clone)]
pub struct MasterClient {
    name: FastStr,
    current_master: FastStr,
    masters: Vec<FastStr>,
    locations: LocationMap,
}

impl MasterClient {
    pub fn new(name: FastStr, masters: Vec<FastStr>) -> Self {
        Self {
            name,
            current_master: FastStr::empty(),
            masters,
            locations: LocationMap::default(),
        }
    }
}

impl MasterClient {
    pub fn current_master(&self) -> &str {
        &self.current_master
    }

    pub async fn try_all_masters(&mut self) -> Result<(), ClientError> {
        for master in self.masters.iter() {
            let name = self.name.to_string();
            let mut interval = tokio::time::interval(Duration::from_secs(1));

            let request_stream = stream! {
                loop {
                    yield client_listen_request(&name);
                    interval.tick().await;
                }
            };
            let client = helyim_client(master)?;
            match client.keep_connected(request_stream).await {
                Ok(response) => {
                    let mut stream = response.into_inner();
                    while let Some(Ok(location)) = stream.next().await {
                        let loc = Location {
                            url: FastStr::new(location.url),
                            public_url: FastStr::new(location.public_url),
                        };

                        for vid in location.new_volume_ids {
                            self.locations.add_location(vid, loc.clone());
                        }

                        for vid in location.deleted_volume_ids {
                            self.locations.delete_location(vid, loc.clone());
                        }

                        if self.current_master.is_empty() {
                            info!("connected to {master}");
                            self.current_master = master.clone();
                        }
                    }
                }
                Err(status) => {
                    error!("keep connected to {master} error: {status}");
                    return Err(ClientError::KeepConnected(master.clone(), status));
                }
            }

            self.current_master = FastStr::empty();
        }

        Ok(())
    }
}

pub fn client_listen_request(name: &str) -> ClientListenRequest {
    ClientListenRequest {
        name: name.to_string(),
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
    Box(Box<dyn std::error::Error + Sync + Send>),
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
