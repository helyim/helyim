use axum::{
    extract::Query,
    http::{header::CONTENT_TYPE, HeaderMap, Method, Uri},
};
use axum_macros::FromRequest;
use bytes::Bytes;
use faststr::FastStr;
use helyim_common::http::HttpError;
use serde::{Deserialize, Serialize};

use crate::entry::Entry;

#[derive(Debug, FromRequest)]
pub struct GetOrHeadExtractor {
    pub uri: Uri,
    pub headers: HeaderMap,
    pub method: Method,
    #[from_request(via(Query))]
    pub list_dir: ListDirQuery,
    pub body: Bytes,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ListDirQuery {
    pub limit: Option<u32>,
    pub last_file_name: Option<FastStr>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListDir {
    pub path: FastStr,
    pub entries: Vec<Entry>,
    pub limit: u32,
    pub last_filename: FastStr,
    pub should_display_load_more: bool,
}

#[derive(Debug, FromRequest)]
pub struct PostExtractor {
    pub uri: Uri,
    pub headers: HeaderMap,
    pub method: Method,
    #[from_request(via(Query))]
    pub query: PostQuery,
    pub body: Bytes,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PostQuery {
    pub replication: Option<FastStr>,
    pub collection: Option<FastStr>,
    pub data_center: Option<FastStr>,
    pub ttl: Option<FastStr>,
    pub max_mb: Option<i32>,
    pub cm: Option<bool>,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct FilerPostResult {
    pub name: String,
    pub size: u32,
    pub error: String,
    pub fid: String,
    pub url: String,
}

#[derive(Debug, FromRequest)]
pub struct DeleteExtractor {
    pub uri: Uri,
    #[from_request(via(Query))]
    pub query: DeleteQuery,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeleteQuery {
    pub recursive: Option<bool>,
}

pub fn parse_boundary(headers: &HeaderMap) -> Result<String, HttpError> {
    match headers.get(CONTENT_TYPE) {
        Some(content_type) => Ok(multer::parse_boundary(content_type.to_str()?)?),
        None => Err(HttpError::Multer(multer::Error::NoBoundary)),
    }
}
