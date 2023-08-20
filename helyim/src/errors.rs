use std::{net::AddrParseError, time::SystemTimeError};

use axum::{
    response::{IntoResponse, Response},
    Json,
};
use hyper::{
    header::{InvalidHeaderName, InvalidHeaderValue, ToStrError},
    StatusCode,
};
use serde_json::json;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// directory errors
    #[error("No free space: {0}")]
    NoFreeSpace(String),
    #[error("No writable volumes")]
    NoWritableVolumes,

    /// storage errors
    #[error("Invalid replica placement: {0}")]
    ParseReplicaPlacement(String),
    #[error("Invalid ttl: {0}")]
    ParseTtl(String),

    /// other errors
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    ParseIntError(#[from] std::num::ParseIntError),
    #[error("{0}")]
    BincodeError(#[from] Box<bincode::ErrorKind>),
    #[error("{0}")]
    Other(#[from] Box<dyn std::error::Error + Sync + Send>),
    #[error("{0}")]
    SerdeJson(#[from] serde_json::Error),
    #[error("{0}")]
    String(String),
    #[error("{0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("{0}")]
    AddrParse(#[from] AddrParseError),
    #[error("{0}")]
    SystemTimeError(#[from] SystemTimeError),

    #[error("{0}")]
    Multer(#[from] multer::Error),

    // http
    #[error("{0}")]
    InvalidHeaderValue(#[from] InvalidHeaderValue),
    #[error("{0}")]
    InvalidHeaderName(#[from] InvalidHeaderName),
    #[error("{0}")]
    ToStr(#[from] ToStrError),
    #[error("{0}")]
    UrlParseError(#[from] url::ParseError),
    #[error("timeout")]
    Timeout,
    #[error("{0}")]
    HyperError(#[from] hyper::Error),
    #[error("{0}")]
    AxumHttpError(#[from] axum::http::Error),

    // tonic
    #[error("{0}")]
    TonicStatus(#[from] tonic::Status),
    #[error("{0}")]
    TonicTransport(#[from] tonic::transport::Error),

    #[error("{0}")]
    SendError(#[from] futures::channel::mpsc::SendError),
    #[error("{0}")]
    BroadcastSendError(#[from] tokio::sync::broadcast::error::SendError<()>),
}

pub type Result<T> = core::result::Result<T, Error>;

impl From<String> for Error {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let error = json!({
            "error": self.to_string()
        });
        let response = (StatusCode::BAD_REQUEST, Json(error));
        response.into_response()
    }
}
