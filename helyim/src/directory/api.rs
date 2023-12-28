use std::{result::Result as StdResult, sync::Arc};

use axum::{extract::State, Json};
use faststr::FastStr;

use crate::{
    errors::Result,
    operation::{
        lookup::{Location, Lookup, LookupRequest},
        AssignRequest, Assignment, ClusterStatus,
    },
    storage::VolumeError,
    topology::{volume_grow::VolumeGrowth, Topology, TopologyRef},
    util::{args::MasterOptions, http::FormOrJson},
};

#[derive(Clone)]
pub struct DirectoryContext {
    pub topology: TopologyRef,
    pub volume_grow: Arc<VolumeGrowth>,
    pub options: Arc<MasterOptions>,
}

pub async fn assign_handler(
    State(ctx): State<DirectoryContext>,
    FormOrJson(request): FormOrJson<AssignRequest>,
) -> StdResult<Json<Assignment>, VolumeError> {
    // TODO: support proxyToLeader
    if !ctx.topology.is_leader().await {
        return Err(VolumeError::String(
            "directory server is not the cluster leader.".to_string(),
        ));
    }

    let count = match request.count {
        Some(n) if n > 1 => n,
        _ => 1,
    };
    let option = Arc::new(request.volume_grow_option(&ctx.options.default_replication)?);

    if !ctx.topology.has_writable_volume(option.clone()).await {
        if ctx.topology.free_volumes().await == 0 {
            return Err(VolumeError::NoFreeSpace("no free volumes".to_string()));
        }
        ctx.volume_grow
            .grow_by_type(option.clone(), ctx.topology.clone())
            .await?;
    }
    let (fid, count, node) = ctx.topology.pick_for_write(count, option).await?;
    let assignment = Assignment {
        fid: fid.to_string(),
        url: node.url(),
        public_url: node.public_url.clone(),
        count,
        error: String::default(),
    };
    Ok(Json(assignment))
}

pub async fn lookup_handler(
    State(ctx): State<DirectoryContext>,
    FormOrJson(request): FormOrJson<LookupRequest>,
) -> StdResult<Json<Lookup>, VolumeError> {
    if request.volume_id.is_empty() {
        return Err(VolumeError::String("volume_id can't be empty".to_string()));
    }
    let mut volume_id = request.volume_id;
    if let Some(idx) = volume_id.rfind(',') {
        volume_id = volume_id[..idx].to_string();
    }
    let mut locations = vec![];
    let data_nodes = ctx
        .topology
        .lookup(
            &request.collection.unwrap_or_default(),
            volume_id.parse::<u32>()?,
        )
        .await;
    match data_nodes {
        Some(nodes) => {
            for dn in nodes.iter() {
                locations.push(Location {
                    url: dn.url(),
                    public_url: dn.public_url.clone(),
                });
            }

            let lookup = Lookup {
                volume_id,
                locations,
                error: FastStr::default(),
            };
            Ok(Json(lookup))
        }
        None => Err(VolumeError::String("cannot find any locations".to_string())),
    }
}

pub async fn dir_status_handler(State(ctx): State<DirectoryContext>) -> Result<Json<Topology>> {
    let topology = ctx.topology.topology();
    Ok(Json(topology))
}

pub async fn cluster_status_handler(State(ctx): State<DirectoryContext>) -> Json<ClusterStatus> {
    let is_leader = ctx.topology.is_leader().await;
    let leader = ctx
        .topology
        .current_leader_address()
        .await
        .unwrap_or_default();
    let peers = ctx.topology.peers().await;

    let status = ClusterStatus {
        is_leader,
        leader,
        peers: peers.into_values().collect(),
    };
    Json(status)
}
