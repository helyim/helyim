use actix_web::{
    post,
    web::{Data, Json},
    Responder,
};
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};

use crate::raft::{NodeId, RaftServer, TypeConfig};

// --- Raft communication

#[post("/raft-vote")]
pub async fn vote(
    app: Data<RaftServer>,
    req: Json<VoteRequest<NodeId>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.vote(req.0).await;
    Ok(Json(res))
}

#[post("/raft-append")]
pub async fn append(
    app: Data<RaftServer>,
    req: Json<AppendEntriesRequest<TypeConfig>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.append_entries(req.0).await;
    Ok(Json(res))
}

#[post("/raft-snapshot")]
pub async fn snapshot(
    app: Data<RaftServer>,
    req: Json<InstallSnapshotRequest<TypeConfig>>,
) -> actix_web::Result<impl Responder> {
    let res = app.raft.install_snapshot(req.0).await;
    Ok(Json(res))
}
