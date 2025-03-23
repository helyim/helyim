use axum::{Json, extract::State};
use helyim_common::types::VolumeId;
use serde::{Deserialize, Serialize};

use crate::{
    node::Node,
    raft::{
        RaftServer,
        types::{ClientWriteError, ClientWriteResponse, OpenRaftError, RaftRequest},
    },
};

#[derive(Debug, Serialize, Deserialize)]
pub struct AppData {
    pub max_volume_id: VolumeId,
}

pub async fn set_max_volume_id_handler(
    State(raft): State<RaftServer>,
    Json(app_data): Json<AppData>,
) -> Json<Result<ClientWriteResponse, OpenRaftError<ClientWriteError>>> {
    let response = raft
        .raft
        .client_write(RaftRequest::max_volume_id(app_data.max_volume_id))
        .await;
    Json(response)
}

pub async fn max_volume_id_handler(State(raft): State<RaftServer>) -> Json<u32> {
    let response = raft
        .state_machine_store
        .state_machine
        .read()
        .await
        .topology()
        .max_volume_id();
    Json(response)
}
