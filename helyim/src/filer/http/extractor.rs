use axum::{
    extract::Query,
    http::{HeaderMap, Method, Uri},
};
use axum_macros::FromRequest;
use bytes::Bytes;
use faststr::FastStr;
use serde::{Deserialize, Serialize};

use crate::filer::entry::Entry;

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
    pub limit: u32,
    pub last_file_name: FastStr,
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
