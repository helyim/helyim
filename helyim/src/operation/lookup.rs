use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

use futures::{
    channel::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    StreamExt,
};
use helyim_proto::{
    helyim_client::HelyimClient, lookup_volume_response::VolumeLocation, LookupVolumeRequest,
    LookupVolumeResponse,
};
use tonic::transport::Channel;
use tracing::info;

use crate::{errors::Result, storage::VolumeId};

pub struct Looker {
    client: HelyimClient<Channel>,
    volumes: HashMap<VolumeId, (VolumeLocation, SystemTime)>,
    timeout: Duration,
    shutdown: async_broadcast::Receiver<()>,
}

impl Looker {
    pub fn new(client: HelyimClient<Channel>, shutdown: async_broadcast::Receiver<()>) -> Looker {
        Looker {
            client,
            // should bigger than volume number
            volumes: HashMap::new(),
            timeout: Duration::from_secs(600),
            shutdown,
        }
    }

    pub async fn lookup(&mut self, vids: &[VolumeId]) -> Result<Vec<VolumeLocation>> {
        let now = SystemTime::now();
        let mut volume_locations = Vec::with_capacity(vids.len());
        let mut volume_ids = vec![];
        for vid in vids {
            match self.volumes.get(vid) {
                Some((location, time)) => {
                    if now.duration_since(*time)? < self.timeout {
                        volume_locations.push(location.clone());
                    } else {
                        volume_ids.push(*vid);
                    }
                }
                None => volume_ids.push(*vid),
            }
        }

        match self.do_lookup(&volume_ids).await {
            Ok(lookup) => {
                for location in lookup.volume_locations {
                    volume_locations.push(location.clone());
                    if !location.error.is_empty() {
                        self.volumes.insert(location.volume_id, (location, now));
                    }
                }
                Ok(volume_locations)
            }
            Err(err) => Err(err),
        }
    }

    async fn do_lookup(&mut self, vids: &[VolumeId]) -> Result<LookupVolumeResponse> {
        let request = LookupVolumeRequest {
            volumes: vids.iter().map(|vid| vid.to_string()).collect(),
            collection: String::default(),
        };
        let response = self.client.lookup_volume(request).await?;
        Ok(response.into_inner())
    }
}

pub enum LookerEvent {
    Lookup(Vec<VolumeId>, oneshot::Sender<Result<Vec<VolumeLocation>>>),
}

pub async fn looker_loop(mut looker: Looker, mut looker_rx: UnboundedReceiver<LookerEvent>) {
    info!("looker event loop starting.");
    loop {
        tokio::select! {
            Some(event) = looker_rx.next() => {
                match event {
                    LookerEvent::Lookup(vid, tx) => {
                        let _ = tx.send(looker.lookup(&vid).await);
                    }
                }
            }
            _ = looker.shutdown.recv() => {
                break;
            }
        }
    }
    info!("looker event loop stopped.");
}

#[derive(Debug, Clone)]
pub struct LookerEventTx(UnboundedSender<LookerEvent>);

impl LookerEventTx {
    pub fn new(tx: UnboundedSender<LookerEvent>) -> Self {
        Self(tx)
    }

    pub async fn lookup(&self, vid: Vec<VolumeId>) -> Result<Vec<VolumeLocation>> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(LookerEvent::Lookup(vid, tx))?;
        rx.await?
    }
}
