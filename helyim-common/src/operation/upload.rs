use std::{collections::HashMap, fmt::Display, str::FromStr};

use bytes::Bytes;
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    multipart::{Form, Part},
    Method,
};
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::{
    http::{get_etag, request, HttpError},
    ttl::Ttl,
};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct UploadResult {
    pub name: String,
    pub size: usize,
    pub error: String,
    pub etag: String,
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

pub async fn upload(
    url: &str,
    filename: String,
    bytes: Vec<u8>,
    mtype: &str,
    pairs: Option<HashMap<&str, &str>>,
) -> Result<UploadResult, HttpError> {
    let part = Part::bytes(bytes).file_name(filename).mime_str(mtype)?;
    let form = Form::new().part("part1", part);

    let mut headers = HeaderMap::new();

    if let Some(pairs) = pairs {
        for (k, v) in pairs {
            headers.insert(HeaderName::from_str(k)?, HeaderValue::from_str(v)?);
        }
    }

    let response = request(
        url,
        Method::POST,
        None,
        None::<Bytes>,
        Some(headers),
        Some(form),
    )
    .await?;

    let etag = get_etag(response.headers())?;
    let body = response.bytes().await?;

    let mut upload: UploadResult = serde_json::from_slice(&body)?;
    if !upload.error.is_empty() {
        error!("upload failed, url: {url}, error: {}", upload.error);
        return Err(anyhow!(
            "upload failed, url: {url}, error: {}",
            upload.error
        ));
    }
    upload.etag = etag;

    Ok(upload)
}
