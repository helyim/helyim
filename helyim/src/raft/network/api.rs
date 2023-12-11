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

pub async fn write_handler(
    State(raft): State<RaftServer>,
    Json(app_data): Json<AppData>,
) -> Result<Json<Result<ClientWriteResponse, OpenRaftError<ClientWriteError>>>, RaftError> {
    let response = raft
        .raft
        .client_write(RaftRequest::max_volume_id(app_data.max_volume_id))
        .await;
    Ok(Json(response))
}

pub async fn read_handler(State(raft): State<RaftServer>) -> Result<Json<AppData>, RaftError> {
    raft.raft.is_leader().await?;
    let state_machine = raft.store.state_machine.read().await;
    let max_volume_id = state_machine.topology().read().await.max_volume_id();
    Ok(Json(AppData { max_volume_id }))
}
