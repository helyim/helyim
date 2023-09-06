use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

use faststr::FastStr;
use futures::{
    channel::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    StreamExt,
};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::{errors::Result, storage::VolumeId, util};

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

pub struct Looker {
    server: FastStr,
    cache: HashMap<VolumeId, (Lookup, SystemTime)>,
    timeout: Duration,
}

impl Looker {
    pub fn new(server: &str) -> Looker {
        Looker {
            server: FastStr::new(server),
            // should bigger than volume number
            cache: HashMap::new(),
            timeout: Duration::from_secs(600),
        }
    }

    pub async fn lookup(&mut self, vid: VolumeId) -> Result<Lookup> {
        let now = SystemTime::now();
        if let Some((look_up, time)) = self.cache.get(&vid) {
            if now.duration_since(*time)? < self.timeout {
                return Ok(look_up.clone());
            }
        }

        match self.do_lookup(vid).await {
            Ok(look_up) => {
                let clone = look_up.clone();
                self.cache.insert(vid, (look_up, now));
                Ok(clone)
            }
            Err(e) => Err(e),
        }
    }

    async fn do_lookup(&mut self, vid: VolumeId) -> Result<Lookup> {
        let vid = vid.to_string();
        let params = vec![("volumeId", vid.as_str())];
        let body = util::get(&format!("http://{}/dir/lookup", self.server), &params).await?;
        Ok(serde_json::from_slice(&body)?)
    }
}

pub enum LookerEvent {
    Lookup(VolumeId, oneshot::Sender<Result<Lookup>>),
}

pub async fn looker_loop(mut looker: Looker, mut looker_rx: UnboundedReceiver<LookerEvent>) {
    info!("looker event loop starting.");
    while let Some(event) = looker_rx.next().await {
        match event {
            LookerEvent::Lookup(vid, tx) => {
                let _ = tx.send(looker.lookup(vid).await);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct LookerEventTx(UnboundedSender<LookerEvent>);

impl LookerEventTx {
    pub fn new(tx: UnboundedSender<LookerEvent>) -> Self {
        Self(tx)
    }

    pub async fn lookup(&self, vid: VolumeId) -> Result<Lookup> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(LookerEvent::Lookup(vid, tx))?;
        rx.await?
    }
}
