use axum::{extract::Query, http::HeaderMap};
use axum_extra::{extract::TypedHeader, headers::Host};
use axum_macros::FromRequest;
use bytes::Bytes;
use faststr::FastStr;
use helyim_common::types::VolumeId;
use hyper::Uri;
use serde::Deserialize;

#[derive(Debug, FromRequest)]
pub struct GetOrHeadExtractor {
    pub uri: Uri,
    pub headers: HeaderMap,
}

#[derive(Debug, FromRequest)]
pub struct PostExtractor {
    // only the last field can implement `FromRequest`
    // other fields must only implement `FromRequestParts`
    pub uri: Uri,
    pub headers: HeaderMap,
    #[from_request(via(Query))]
    pub query: StorageQuery,
    pub body: Bytes,
}

#[derive(Debug, FromRequest)]
pub struct DeleteExtractor {
    // only the last field can implement `FromRequest`
    // other fields must only implement `FromRequestParts`
    pub uri: Uri,
    #[from_request(via(TypedHeader))]
    pub host: Host,
    #[from_request(via(Query))]
    pub query: StorageQuery,
}

#[derive(Debug, Deserialize)]
pub struct StorageQuery {
    pub r#type: Option<FastStr>,
    // is chunked file
    pub cm: Option<bool>,
    pub ttl: Option<FastStr>,
    // last modified
    pub ts: Option<u64>,
}

#[derive(Debug, FromRequest)]
pub struct ErasureCodingExtractor {
    #[from_request(via(Query))]
    pub query: ErasureCodingQuery,
}

#[derive(Debug, Deserialize)]
pub struct ErasureCodingQuery {
    pub volume: VolumeId,
    pub collection: Option<FastStr>,
}
