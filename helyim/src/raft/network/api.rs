use axum::{extract::State, Json};
use serde::{Deserialize, Serialize};

use crate::{
    raft::{
        types::{ClientWriteError, ClientWriteResponse, OpenRaftError, RaftError, RaftRequest},
        RaftServer,
    },
    storage::VolumeId,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct AppData {
    pub max_volume_id: VolumeId,
}

pub async fn set_max_volume_id_handler(
    State(raft): State<RaftServer>,
    Json(app_data): Json<AppData>,
) -> Result<Json<Result<ClientWriteResponse, OpenRaftError<ClientWriteError>>>, RaftError> {
    let response = raft
        .raft
        .client_write(RaftRequest::max_volume_id(app_data.max_volume_id))
        .await;
    Ok(Json(response))
}
