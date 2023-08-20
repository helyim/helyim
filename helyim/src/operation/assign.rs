use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Assignment {
    pub fid: String,
    pub url: String,
    pub public_url: String,
    pub count: u64,
    pub error: String,
}
