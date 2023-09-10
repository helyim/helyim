use faststr::FastStr;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Assignment {
    pub fid: String,
    pub url: String,
    pub public_url: FastStr,
    pub count: u64,
    pub error: String,
}
