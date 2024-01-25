use std::{collections::HashMap, fmt::Display};

use serde::{Deserialize, Serialize};

use crate::storage::Ttl;

#[derive(Default, Serialize, Deserialize)]
pub struct Upload {
    pub name: String,
    pub size: usize,
    pub error: String,
}

pub struct ParseUpload {
    pub filename: String,
    pub data: Vec<u8>,
    pub mime_type: String,
    pub pair_map: HashMap<String, String>,
    pub modified_time: u64,
    pub ttl: Ttl,
    pub is_chunked_file: bool,
}

impl Display for ParseUpload {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "filename: {}, data_len: {}, mime_type: {}, ttl minutes: {}, is_chunked_file: {}",
            self.filename,
            self.data.len(),
            self.mime_type,
            self.ttl.minutes(),
            self.is_chunked_file
        )
    }
}
