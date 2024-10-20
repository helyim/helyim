use axum::{
    response::{IntoResponse, Response},
    Json,
};
use helyim_common::{parser::ParseError, types::VolumeId};
use serde_json::json;
use tonic::{codegen::http::StatusCode, Status};

use crate::ShardId;

#[derive(thiserror::Error, Debug)]
pub enum EcVolumeError {
    #[error("Io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Error: {0}")]
    Box(#[from] Box<dyn std::error::Error + Sync + Send>),

    #[error("{0}")]
    String(String),

    #[error("Parse error: {0}")]
    Parse(#[from] ParseError),
    #[error("Erasure coding error: {0}")]
    ErasureCoding(#[from] reed_solomon_erasure::Error),

    #[error("Ec shard error: {0}")]
    EcShard(#[from] EcShardError),

    #[error("Shard {1} not found in volume {0}")]
    ShardNotFound(VolumeId, ShardId),

    #[error("Join error: {0}")]
    TokioTaskJoin(#[from] tokio::task::JoinError),

    #[error("Future channel send error: {0}")]
    FutureSendError(#[from] futures::channel::mpsc::SendError),
    #[error("Future channel try send error: {0}")]
    FutureTrySendError(
        #[from]
        futures::channel::mpsc::TrySendError<
            helyim_proto::directory::VolumeEcShardInformationMessage,
        >,
    ),

    #[error("Tonic error: {0}")]
    Tonic(#[from] Status),
}

impl From<EcVolumeError> for Status {
    fn from(value: EcVolumeError) -> Self {
        Status::internal(value.to_string())
    }
}

impl IntoResponse for EcVolumeError {
    fn into_response(self) -> Response {
        let error = self.to_string();
        let error = json!({
            "error": error
        });
        let response = (StatusCode::BAD_REQUEST, Json(error));
        response.into_response()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum EcShardError {
    #[error("Io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("error: {0}")]
    Box(#[from] Box<dyn std::error::Error + Sync + Send>),

    #[error("{0}")]
    String(String),

    #[error("Erasure coding error: {0}")]
    ErasureCoding(#[from] reed_solomon_erasure::Error),
    #[error("Only {0} shards found but {0} required")]
    Underflow(usize, usize),
}

impl From<EcShardError> for Status {
    fn from(value: EcShardError) -> Self {
        Status::internal(value.to_string())
    }
}