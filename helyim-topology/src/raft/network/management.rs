use std::collections::{BTreeSet, HashMap};

use axum::{Json, extract::State};
use faststr::FastStr;
use openraft::{BasicNode, RaftMetrics};

use crate::raft::{
    RaftServer,
    types::{ClientWriteError, ClientWriteResponse, InitializeError, NodeId, OpenRaftError},
};

pub async fn add_learner_handler(
    State(raft): State<RaftServer>,
    Json((node_id, addr)): Json<(NodeId, String)>,
) -> Json<Result<ClientWriteResponse, OpenRaftError<ClientWriteError>>> {
    Json(raft.add_learner(node_id, &addr).await)
}

pub async fn change_membership_handler(
    State(raft): State<RaftServer>,
    Json(members): Json<BTreeSet<NodeId>>,
) -> Json<Result<ClientWriteResponse, OpenRaftError<ClientWriteError>>> {
    Json(raft.change_membership(members).await)
}

pub async fn init_handler(
    State(raft): State<RaftServer>,
    Json(members): Json<HashMap<NodeId, FastStr>>,
) -> Json<Result<(), OpenRaftError<InitializeError>>> {
    Json(raft.initialize(members).await)
}

pub async fn metrics_handler(
    State(raft): State<RaftServer>,
) -> Json<Result<RaftMetrics<NodeId, BasicNode>, OpenRaftError>> {
    let metrics = raft.raft.metrics().borrow().clone();
    Json(Ok(metrics))
}
