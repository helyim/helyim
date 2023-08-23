use std::sync::Arc;

use axum::{
    extract::{Query, State},
    Json,
};
use futures::lock::Mutex;
use serde::Deserialize;
use validator::Validate;

use crate::{
    directory::{
        topology::{
            topology::TopologyEventTx,
            volume_grow::{VolumeGrowthEvent, VolumeGrowthEventTx},
        },
        Topology, VolumeGrowOption, VolumeGrowth,
    },
    errors::{Error, Result},
    operation::{Assignment, ClusterStatus, Location, Lookup},
    storage::{ReplicaPlacement, Ttl},
};

#[derive(Debug, Clone)]
pub struct DirectoryContext {
    pub topology: TopologyEventTx,
    pub volume_grow: VolumeGrowthEventTx,
    pub default_replica_placement: ReplicaPlacement,
    pub ip: String,
    pub port: u16,
}

#[derive(Debug, Deserialize, Validate)]
pub struct AssignRequest {
    count: Option<u64>,
    #[validate(length(min = 3, max = 3))]
    replication: Option<String>,
    ttl: Option<String>,
    preallocate: Option<i64>,
    collection: Option<String>,
    data_center: Option<String>,
    rack: Option<String>,
    data_node: Option<String>,
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
    State(ctx): State<DirectoryContext>,
    Query(request): Query<AssignRequest>,
) -> Result<Json<Assignment>> {
    let mut count = 1;
    if let Some(c) = request.count {
        if c > 1 {
            count = c;
        }
    }
    let option = request.volume_grow_option()?;

    if !ctx.topology.has_writable_volume(option.clone()).await? {
        if ctx.topology.free_volumes().await? <= 0 {
            return Err(Error::NoFreeSpace("no free volumes".to_string()));
        }
        ctx.volume_grow
            .grow_by_type(option.clone(), ctx.topology.clone())
            .await?;
    }
    let (fid, count, node) = ctx.topology.pick_for_write(count, option).await?;
    let assignment = Assignment {
        fid: fid.to_string(),
        url: node.url().await?,
        public_url: node.public_url().await?,
        count,
        error: String::from(""),
    };
    Ok(Json(assignment))
}

#[derive(Debug, Deserialize)]
pub struct LookupRequest {
    volume_id: String,
    collection: Option<String>,
}

pub async fn lookup_handler(
    State(ctx): State<DirectoryContext>,
    Query(request): Query<LookupRequest>,
) -> Result<Json<Lookup>> {
    if request.volume_id.is_empty() {
        return Err(Error::String("volume_id can't be empty".to_string()));
    }
    let mut volume_id = request.volume_id;
    if let Some(idx) = volume_id.rfind(',') {
        volume_id = volume_id[..idx].to_string();
    }
    let mut locations = vec![];
    let collection = request.collection.unwrap_or_default();
    match ctx
        .topology
        .lookup(collection, volume_id.parse::<u32>()?)
        .await?
    {
        Some(nodes) => {
            for dn in nodes.iter() {
                locations.push(Location {
                    url: dn.url().await?,
                    public_url: dn.public_url().await?,
                });
            }

            let lookup = Lookup {
                volume_id,
                locations,
                error: String::new(),
            };
            Ok(Json(lookup))
        }
        None => Err(Error::String("cannot find any locations".to_string())),
    }
}

pub async fn dir_status_handler(State(ctx): State<DirectoryContext>) -> Result<Json<Topology>> {
    let topology = ctx.topology.topology().await?;
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
