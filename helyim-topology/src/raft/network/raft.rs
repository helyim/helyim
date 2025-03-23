use axum::{Json, extract::State};
use openraft::{
    error::InstallSnapshotError,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};

use crate::raft::{
    RaftServer,
    types::{NodeId, OpenRaftError, TypeConfig},
};

pub async fn vote_handler(
    State(raft): State<RaftServer>,
    Json(vote): Json<VoteRequest<NodeId>>,
) -> Json<Result<VoteResponse<NodeId>, OpenRaftError>> {
    let response = raft.raft.vote(vote).await;
    Json(response)
}

pub async fn append_entries_handler(
    State(raft): State<RaftServer>,
    Json(append): Json<AppendEntriesRequest<TypeConfig>>,
) -> Json<Result<AppendEntriesResponse<NodeId>, OpenRaftError>> {
    let response = raft.raft.append_entries(append).await;
    Json(response)
}

pub async fn install_snapshot_handler(
    State(raft): State<RaftServer>,
    Json(snapshot): Json<InstallSnapshotRequest<TypeConfig>>,
) -> Json<Result<InstallSnapshotResponse<NodeId>, OpenRaftError<InstallSnapshotError>>> {
    let response = raft.raft.install_snapshot(snapshot).await;
    Json(response)
}
