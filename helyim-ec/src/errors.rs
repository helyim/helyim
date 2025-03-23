use axum::{
    Json,
    response::{IntoResponse, Response},
};
use helyim_common::{
    parser::ParseError,
    types::{NeedleId, VolumeId},
};
use serde_json::json;
use tonic::{Status, codegen::http::StatusCode};

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
    #[error("Needle {1} not found in volume {0}")]
    NeedleNotFound(VolumeId, NeedleId),
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
    #[error("Erasure coding error: {0}")]
    ErasureCoding(#[from] reed_solomon_erasure::Error),
    #[error("Only {0} shards found but {0} required")]
    Underflow(usize, usize),
    #[error("ec shard size expected {0} but actually is {1}")]
    UnexpectedEcShardSize(usize, usize),
    #[error("unexpected block size {0}, buffer size {1}")]
    UnexpectedBlockSize(usize, usize),
}

impl From<EcShardError> for Status {
    fn from(value: EcShardError) -> Self {
        Status::internal(value.to_string())
    }
}
