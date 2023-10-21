use std::sync::Arc;

use axum::{
    extract::{Query, State},
    Json,
};
use faststr::FastStr;
use serde::Deserialize;

use crate::{
    errors::{Error, Result},
    operation::{Assignment, ClusterStatus},
    storage::{ReplicaPlacement, Ttl},
    topology::{
        volume_grow::{VolumeGrowOption, VolumeGrowth},
        Topology,
    },
};

#[derive(Clone)]
pub struct DirectoryContext {
    pub topology: Arc<Topology>,
    pub volume_grow: VolumeGrowth,
    pub default_replica_placement: ReplicaPlacement,
    pub ip: FastStr,
    pub port: u16,
}

#[derive(Debug, Deserialize)]
pub struct AssignRequest {
    count: Option<u64>,
    replication: Option<FastStr>,
    ttl: Option<FastStr>,
    preallocate: Option<i64>,
    collection: Option<FastStr>,
    data_center: Option<FastStr>,
    rack: Option<FastStr>,
    data_node: Option<FastStr>,
}

impl AssignRequest {
    pub fn volume_grow_option(self) -> Result<VolumeGrowOption> {
        let mut option = VolumeGrowOption::default();
        if let Some(replication) = self.replication {
            option.replica_placement = ReplicaPlacement::new(&replication)?;
        }
        if let Some(ttl) = self.ttl {
            option.ttl = Ttl::new(&ttl)?;
        }
        if let Some(preallocate) = self.preallocate {
            option.preallocate = preallocate;
        }
        if let Some(collection) = self.collection {
            option.collection = collection;
        }
        if let Some(data_center) = self.data_center {
            option.data_center = data_center;
        }
        if let Some(rack) = self.rack {
            option.rack = rack;
        }
        if let Some(data_node) = self.data_node {
            option.data_node = data_node;
        }
        Ok(option)
    }
}

pub async fn assign_handler(
    State(mut ctx): State<DirectoryContext>,
    Query(request): Query<AssignRequest>,
) -> Result<Json<Assignment>> {
    let count = match request.count {
        Some(n) if n > 1 => n,
        _ => 1,
    };
    let option = request.volume_grow_option()?;

    if !ctx.topology.has_writable_volume(option.clone()).await {
        if ctx.topology.free_volumes().await <= 0 {
            return Err(Error::NoFreeSpace("no free volumes".to_string()));
        }
        ctx.volume_grow
            .grow_by_type(option.clone(), ctx.topology.clone())
            .await?;
    }
    let (fid, count, node) = ctx.topology.pick_for_write(count, option).await?;
    let assignment = Assignment {
        fid: fid.to_string(),
        url: node.read().await.url(),
        public_url: node.read().await.public_url.clone(),
        count,
        error: String::default(),
    };
    Ok(Json(assignment))
}

pub async fn dir_status_handler(State(ctx): State<DirectoryContext>) -> Result<Json<Topology>> {
    let topology = ctx.topology.topology();
    Ok(Json(topology))
}

pub async fn cluster_status_handler(State(ctx): State<DirectoryContext>) -> Json<ClusterStatus> {
    let status = ClusterStatus {
        is_leader: true,
        leader: format!("{}:{}", ctx.ip, ctx.port),
        peers: vec![],
    };
    Json(status)
}
