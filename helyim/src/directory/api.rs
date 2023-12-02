use std::{result::Result as StdResult, sync::Arc};

use axum::{extract::State, Json};
use faststr::FastStr;
use tracing::error;

use crate::{
    errors::Result,
    operation::{
        lookup::{Location, Lookup, LookupRequest},
        AssignRequest, Assignment, ClusterStatus,
    },
    storage::VolumeError,
    topology::{volume_grow::VolumeGrowth, Topology, TopologyRef},
    util::FormOrJson,
};

#[derive(Clone)]
pub struct DirectoryContext {
    pub topology: TopologyRef,
    pub volume_grow: Arc<VolumeGrowth>,
    pub default_replication: FastStr,
    pub ip: FastStr,
    pub port: u16,
}

pub async fn assign_handler(
    State(ctx): State<DirectoryContext>,
    FormOrJson(request): FormOrJson<AssignRequest>,
) -> StdResult<Json<Assignment>, VolumeError> {
    let count = match request.count {
        Some(n) if n > 1 => n,
        _ => 1,
    };
    let option = Arc::new(request.volume_grow_option(&ctx.default_replication)?);

    if !ctx
        .topology
        .write()
        .await
        .has_writable_volume(option.clone())
        .await
    {
        if ctx.topology.read().await.free_volumes().await <= 0 {
            error!("find no free volumes");
            return Err(VolumeError::NoFreeSpace("no free volumes".to_string()));
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

pub async fn lookup_handler(
    State(ctx): State<DirectoryContext>,
    FormOrJson(request): FormOrJson<LookupRequest>,
) -> StdResult<Json<Lookup>, VolumeError> {
    if request.volume_id.is_empty() {
        error!("volume_id can't be empty");
        return Err(VolumeError::String("volume_id can't be empty".to_string()));
    }
    let mut volume_id = request.volume_id;
    if let Some(idx) = volume_id.rfind(',') {
        volume_id = volume_id[..idx].to_string();
    }
    let mut locations = vec![];
    let data_nodes = ctx
        .topology
        .write()
        .await
        .lookup(
            request.collection.unwrap_or_default(),
            volume_id.parse::<u32>()?,
        )
        .await;
    match data_nodes {
        Some(nodes) => {
            for dn in nodes.iter() {
                locations.push(Location {
                    url: dn.read().await.url(),
                    public_url: dn.read().await.public_url.clone(),
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
