use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

use crate::storage::{erasure_coding::ShardId, NeedleError, VolumeError, VolumeId};

#[derive(thiserror::Error, Debug)]
pub enum EcVolumeError {
    #[error("Io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Error: {0}")]
    BoxError(#[from] Box<dyn std::error::Error + Sync + Send>),
    #[error("ParseInt error: {0}")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("{0}")]
    String(String),

    #[error("Erasure coding error: {0}")]
    ErasureCoding(#[from] reed_solomon_erasure::Error),

    #[error("Volume error: {0}")]
    Volume(#[from] VolumeError),
    #[error("Needle error: {0}")]
    Needle(#[from] NeedleError),
    #[error("Ec shard error: {0}")]
    EcShard(#[from] EcShardError),

    #[error("shard {1} not found in volume {0}")]
    ShardNotFound(VolumeId, ShardId),

    #[error("Join error: {0}")]
    TokioTaskJoin(#[from] tokio::task::JoinError),

    #[error("Future channel send error: {0}")]
    FutureSendError(#[from] futures::channel::mpsc::SendError),
    #[error("Future channel try send error: {0}")]
    FutureTrySendError(
        #[from] futures::channel::mpsc::TrySendError<helyim_proto::VolumeEcShardInformationMessage>,
    ),

    #[error("Tonic transport error: {0}")]
    TonicTransport(#[from] tonic::transport::Error),
    #[error("Tonic status: {0}")]
    TonicStatus(#[from] tonic::Status),
}

impl From<EcVolumeError> for tonic::Status {
    fn from(value: EcVolumeError) -> Self {
        tonic::Status::internal(value.to_string())
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
    BoxError(#[from] Box<dyn std::error::Error + Sync + Send>),

    #[error("{0}")]
    String(String),

    #[error("Erasure coding error: {0}")]
    ErasureCoding(#[from] reed_solomon_erasure::Error),
    #[error("only {0} shards found but {0} required")]
    Underflow(usize, usize),
}

impl From<EcShardError> for tonic::Status {
    fn from(value: EcShardError) -> Self {
        tonic::Status::internal(value.to_string())
    }
}
