use std::collections::{BTreeMap, BTreeSet};

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
    Json(learner): Json<(NodeId, String)>,
) -> Result<Json<Result<ClientWriteResponse, OpenRaftError<ClientWriteError>>>, RaftError> {
    let node_id = learner.0;
    let node = BasicNode { addr: learner.1 };
    let res = raft.raft.add_learner(node_id, node, true).await;
    Ok(Json(res))
}

pub async fn change_membership_handler(
    State(raft): State<RaftServer>,
    Json(members): Json<BTreeSet<NodeId>>,
) -> Result<Json<Result<ClientWriteResponse, OpenRaftError<ClientWriteError>>>, RaftError> {
    let res = raft.raft.change_membership(members, false).await;
    println!("{:?}", res);
    Ok(Json(res))
}

pub async fn init_handler(
    State(raft): State<RaftServer>,
) -> Result<Json<Result<(), OpenRaftError<InitializeError>>>, RaftError> {
    let mut nodes = BTreeMap::new();
    nodes.insert(
        raft.id,
        BasicNode {
            addr: raft.addr.clone(),
        },
    );
    let res = raft.raft.initialize(nodes).await;
    Ok(Json(res))
}

pub async fn metrics_handler(
    State(raft): State<RaftServer>,
) -> Json<RaftMetrics<NodeId, BasicNode>> {
    let metrics = raft.raft.metrics().borrow().clone();
    Json(metrics)
}
