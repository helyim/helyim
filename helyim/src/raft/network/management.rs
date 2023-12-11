use std::collections::{BTreeMap, BTreeSet};

use actix_web::{
    get, post,
    web::{Data, Json},
    Responder,
};
use openraft::{error::Infallible, BasicNode, RaftMetrics};

use crate::raft::{types::NodeId, RaftServer};

// --- Cluster management

/// Add a node as **Learner**.
///
/// A Learner receives log replication from the leader but does not vote.
/// This should be done before adding a node as a member into the cluster
/// (by calling `change-membership`)
#[post("/add-learner")]
pub async fn add_learner(
    app: Data<RaftServer>,
    req: Json<(NodeId, String)>,
) -> actix_web::Result<impl Responder> {
    let node_id = req.0 .0;
    let node = BasicNode {
        addr: req.0 .1.clone(),
    };
    let res = app.raft.add_learner(node_id, node, true).await;
    Ok(Json(res))
}

/// Changes specified learners to members, or remove members.
#[post("/change-membership")]
pub async fn change_membership(
    app: Data<RaftServer>,
    req: Json<BTreeSet<NodeId>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.change_membership(req.0, false).await;
    Ok(Json(res))
}

/// Initialize a single-node cluster.
#[post("/init")]
pub async fn init(app: Data<RaftServer>) -> actix_web::Result<impl Responder> {
    let mut nodes = BTreeMap::new();
    nodes.insert(
        app.id,
        BasicNode {
            addr: app.addr.clone(),
        },
    );
    let res = app.raft.initialize(nodes).await;
    Ok(Json(res))
}

/// Get the latest metrics of the cluster
#[get("/metrics")]
pub async fn metrics(app: Data<RaftServer>) -> actix_web::Result<impl Responder> {
    let metrics = app.raft.metrics().borrow().clone();

    let res: Result<RaftMetrics<NodeId, BasicNode>, Infallible> = Ok(metrics);
    Ok(Json(res))
}
