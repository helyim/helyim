use axum::{
    extract::{Query, State},
    Json,
};
use faststr::FastStr;
use serde::Deserialize;

use crate::{
    directory::{
        topology::{topology::TopologyEventTx, volume_grow::VolumeGrowthEventTx},
        Topology, VolumeGrowOption,
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
    State(ctx): State<DirectoryContext>,
    Query(request): Query<AssignRequest>,
) -> Result<Json<Assignment>> {
    let count = match request.count {
        Some(n) if n > 1 => n,
        _ => 1,
    };
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
        error: FastStr::empty(),
    };
    Ok(Json(assignment))
}

#[derive(Debug, Deserialize)]
pub struct LookupRequest {
    volume_id: FastStr,
    collection: Option<FastStr>,
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
        volume_id = volume_id.slice_ref(&volume_id[..idx]);
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
                error: FastStr::empty(),
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
