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
        Topology, TopologyRef,
    },
};

#[derive(Clone)]
pub struct DirectoryContext {
    pub topology: TopologyRef,
    pub volume_grow: Arc<VolumeGrowth>,
    pub default_replication: FastStr,
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
    pub fn volume_grow_option(self, ctx: &DirectoryContext) -> Result<VolumeGrowOption> {
        let mut option = VolumeGrowOption::default();
        match self.replication {
            Some(mut replication) => {
                if replication.is_empty() {
                    replication = ctx.default_replication.clone();
                }
                option.replica_placement = ReplicaPlacement::new(&replication)?;
            }
            None => {
                option.replica_placement = ReplicaPlacement::new(&ctx.default_replication)?;
            }
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
    State(ctx): State<DirectoryContext>,
    Query(request): Query<AssignRequest>,
) -> Result<Json<Assignment>> {
    let count = match request.count {
        Some(n) if n > 1 => n,
        _ => 1,
    };
    let option = Arc::new(request.volume_grow_option(&ctx)?);

    if !ctx
        .topology
        .write()
        .await
        .has_writable_volume(option.clone())
        .await?
    {
        if ctx.topology.read().await.free_volumes().await? <= 0 {
            return Err(Error::NoFreeSpace("no free volumes".to_string()));
        }
        ctx.volume_grow
            .grow_by_type(option.clone(), ctx.topology.clone())
            .await?;
    }
    let (fid, count, node) = ctx
        .topology
        .write()
        .await
        .pick_for_write(count, option)
        .await?;
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
    let topology = ctx.topology.read().await.topology();
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
