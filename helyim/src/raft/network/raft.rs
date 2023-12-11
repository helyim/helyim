use axum::{extract::State, Json};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use crate::raft::{
    types::{NodeId, RaftError, TypeConfig},
    RaftServer,
};

pub async fn vote_handler(
    State(raft): State<RaftServer>,
    Json(vote): Json<VoteRequest<NodeId>>,
) -> Result<Json<VoteResponse<NodeId>>, RaftError> {
    let response = raft.raft.vote(vote).await?;
    Ok(Json(response))
}

pub async fn append_handler(
    State(raft): State<RaftServer>,
    Json(append): Json<AppendEntriesRequest<TypeConfig>>,
) -> Result<Json<AppendEntriesResponse<NodeId>>, RaftError> {
    let response = raft.raft.append_entries(append).await?;
    Ok(Json(response))
}

pub async fn snapshot_handler(
    State(raft): State<RaftServer>,
    Json(snapshot): Json<InstallSnapshotRequest<TypeConfig>>,
) -> Result<Json<InstallSnapshotResponse<NodeId>>, RaftError> {
    let response = raft.raft.install_snapshot(snapshot).await?;
    Ok(Json(response))
}
