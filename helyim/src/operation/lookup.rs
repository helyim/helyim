use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

use serde::{Deserialize, Serialize};

use crate::{errors::Result, util};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Location {
    pub url: String,
    pub public_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Lookup {
    pub volume_id: String,
    pub locations: Vec<Location>,
    pub error: String,
}

pub struct Looker {
    server: String,
    cache: HashMap<String, (Lookup, SystemTime)>,
    timeout: Duration,
}

impl Looker {
    pub fn new(server: &str) -> Looker {
        Looker {
            server: String::from(server),
            // should bigger than volume number
            cache: HashMap::new(),
            timeout: Duration::from_secs(600),
        }
    }

    pub async fn lookup(&mut self, vid: &str) -> Result<Lookup> {
        let now = SystemTime::now();
        if let Some((look_up, time)) = self.cache.get(&String::from(vid)) {
            if now.duration_since(*time)?.lt(&self.timeout) {
                return Ok(look_up.clone());
            }
        }

        match self.do_lookup(vid).await {
            Ok(look_up) => {
                let clone = look_up.clone();
                self.cache.insert(String::from(vid), (look_up, now));
                Ok(clone)
            }
            Err(e) => Err(e),
        }
    }

    async fn do_lookup(&mut self, vid: &str) -> Result<Lookup> {
        let params: Vec<(&str, &str)> = vec![("volumeId", vid)];
        let body = util::get(&format!("http://{}/dir/lookup", self.server), &params).await?;
        Ok(serde_json::from_slice(&body)?)
    }
}
