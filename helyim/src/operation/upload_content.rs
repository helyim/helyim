use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize)]
pub struct Upload {
    pub name: String,
    pub size: u32,
    pub error: String,
}
