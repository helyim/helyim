use faststr::FastStr;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LookupRequest {
    pub volume_id: String,
    pub collection: Option<String>,
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
