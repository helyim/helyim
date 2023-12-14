use std::collections::BTreeSet;

use axum::{extract::State, Json};
use openraft::{BasicNode, RaftMetrics};

use crate::raft::{
    types::{
        ClientWriteError, ClientWriteResponse, InitializeError, NodeId, OpenRaftError, RaftError,
    },
    RaftServer,
};

pub async fn add_learner_handler(
    State(raft): State<RaftServer>,
    Json((node_id, addr)): Json<(NodeId, String)>,
) -> Result<Json<Result<ClientWriteResponse, OpenRaftError<ClientWriteError>>>, RaftError> {
    Ok(Json(raft.add_learner(node_id, &addr).await))
}

pub async fn change_membership_handler(
    State(raft): State<RaftServer>,
    Json(members): Json<BTreeSet<NodeId>>,
) -> Result<Json<Result<ClientWriteResponse, OpenRaftError<ClientWriteError>>>, RaftError> {
    Ok(Json(raft.change_membership(members).await))
}

pub async fn init_handler(
    State(raft): State<RaftServer>,
) -> Result<Json<Result<(), OpenRaftError<InitializeError>>>, RaftError> {
    Ok(Json(raft.initialize().await))
}

pub async fn metrics_handler(
    State(raft): State<RaftServer>,
) -> Json<RaftMetrics<NodeId, BasicNode>> {
    let metrics = raft.raft.metrics().borrow().clone();
    Json(metrics)
}
