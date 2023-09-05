use std::{net::AddrParseError, time::SystemTimeError};

use axum::{
    response::{IntoResponse, Response},
    Json,
};
use futures::channel::mpsc::TrySendError;
use hyper::{
    header::{InvalidHeaderName, InvalidHeaderValue, ToStrError},
    StatusCode,
};
use serde_json::json;
use tracing::error;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// directory errors
    #[error("No free space: {0}")]
    NoFreeSpace(String),
    #[error("No writable volumes")]
    NoWritableVolumes,
    #[error("data integrity error: {0}")]
    DataIntegrity(String),
    #[error("Cookie not match, needle cookie is {0} but got {1}")]
    CookieNotMatch(u32, u32),

    /// storage errors
    #[error("Invalid replica placement: {0}")]
    ParseReplicaPlacement(String),
    #[error("Invalid ttl: {0}")]
    ParseTtl(String),
    #[error("Invalid file id: {0}")]
    InvalidFid(String),

    /// other errors
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("parse integer error: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),
    #[error("bincode error: {0}")]
    BincodeError(#[from] Box<bincode::ErrorKind>),
    #[error("other error: {0}")]
    Other(#[from] Box<dyn std::error::Error + Sync + Send>),
    #[error("serde json error: {0}")]
    SerdeJson(#[from] serde_json::Error),
    #[error("raw error: {0}")]
    String(String),
    #[error("utf8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("addr parse error: {0}")]
    AddrParse(#[from] AddrParseError),
    #[error("system time error: {0}")]
    SystemTimeError(#[from] SystemTimeError),
    #[error("crc error, read: {0}, calculate: {1}, may be data on disk corrupted")]
    Crc(u32, u32),
    #[error("unsupported version: {0}")]
    UnsupportedVersion(u8),

    #[error("multer error: {0}")]
    Multer(#[from] multer::Error),

    #[error("errno: {0}")]
    Errno(#[from] rustix::io::Errno),

    // http
    #[error("invalid header value: {0}")]
    InvalidHeaderValue(#[from] InvalidHeaderValue),
    #[error("invalid header name: {0}")]
    InvalidHeaderName(#[from] InvalidHeaderName),
    #[error("to str error: {0}")]
    ToStr(#[from] ToStrError),
    #[error("url parse error: {0}")]
    UrlParseError(#[from] url::ParseError),
    #[error("timeout")]
    Timeout,
    #[error("hyper error: {0}")]
    HyperError(#[from] hyper::Error),
    #[error("axum http error: {0}")]
    AxumHttpError(#[from] axum::http::Error),

    // tonic
    #[error("tonic status: {0}")]
    TonicStatus(#[from] tonic::Status),
    #[error("tonic transport error: {0}")]
    TonicTransport(#[from] tonic::transport::Error),

    #[error("futures channel send error: {0}")]
    SendError(#[from] futures::channel::mpsc::SendError),
    #[error("broadcast channel closed")]
    BroadcastSendError(#[from] tokio::sync::broadcast::error::SendError<()>),
    #[error("oneshot channel canceled")]
    OneshotCanceled(#[from] futures::channel::oneshot::Canceled),
}

pub type Result<T> = core::result::Result<T, Error>;

impl From<String> for Error {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let error = self.to_string();
        error!("axum response: {error}");

        let error = json!({
            "error": error
        });
        let response = (StatusCode::BAD_REQUEST, Json(error));
        response.into_response()
    }
}

impl<T> From<TrySendError<T>> for Error {
    fn from(value: TrySendError<T>) -> Self {
        Error::String(value.to_string())
    }
}
