use std::net::AddrParseError;

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

use crate::storage::{NeedleError, VolumeError};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Volume error: {0}")]
    Volume(#[from] VolumeError),
    #[error("Needle error: {0}")]
    Needle(#[from] NeedleError),

    /// other errors
    #[error("Io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Parse integer error: {0}")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("Bincode error: {0}")]
    Bincode(#[from] Box<bincode::ErrorKind>),
    #[error("Other error: {0}")]
    Other(#[from] Box<dyn std::error::Error + Sync + Send>),
    #[error("Serde json error: {0}")]
    SerdeJson(#[from] serde_json::Error),
    #[error("{0}")]
    String(String),
    #[error("Utf8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("Addr parse error: {0}")]
    AddrParse(#[from] AddrParseError),
    #[error("Nom error: {0}")]
    Nom(String),
    #[error("Multer error: {0}")]
    Multer(#[from] multer::Error),

    #[error("Errno: {0}")]
    Errno(#[from] rustix::io::Errno),

    #[error("Snowflake error: {0}")]
    Snowflake(#[from] sonyflake::Error),

    // http
    #[error("Invalid header value: {0}")]
    InvalidHeaderValue(#[from] InvalidHeaderValue),
    #[error("Invalid header name: {0}")]
    InvalidHeaderName(#[from] InvalidHeaderName),
    #[error("Tostr error: {0}")]
    ToStr(#[from] ToStrError),
    #[error("Url parse error: {0}")]
    UrlParse(#[from] url::ParseError),
    #[error("Timeout")]
    Timeout,
    #[error("Hyper error: {0}")]
    Hyper(#[from] hyper::Error),
    #[error("Axum http error: {0}")]
    AxumHttp(#[from] axum::http::Error),

    // tonic
    #[error("Tonic status: {0}")]
    TonicStatus(#[from] tonic::Status),
    #[error("Tonic transport error: {0}")]
    TonicTransport(#[from] tonic::transport::Error),

    #[error("Broadcast channel closed")]
    BroadcastSend(#[from] async_broadcast::SendError<()>),
    #[error("JoinHandle error: {0}")]
    TaskJoin(#[from] tokio::task::JoinError),
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

impl From<Error> for tonic::Status {
    fn from(value: Error) -> Self {
        tonic::Status::internal(value.to_string())
    }
}
