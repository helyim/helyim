mod extractor;
use std::sync::Arc;

use axum::{Json, extract::State};
pub use extractor::require_leader;
use faststr::FastStr;
use helyim_common::{http::FormOrJson, operation::ClusterStatus};
use helyim_topology::{
    Topology, TopologyError, TopologyRef, node::Node, volume_grow::VolumeGrowth,
};
use tracing::debug;

use crate::{
    MasterOptions,
    operation::{AssignRequest, Assignment, Location, Lookup, LookupRequest},
};

#[derive(Clone)]
pub struct DirectoryState {
    pub topology: TopologyRef,
    pub volume_grow: VolumeGrowth,
    pub options: Arc<MasterOptions>,
}

pub async fn assign_handler(
    State(state): State<DirectoryState>,
    FormOrJson(request): FormOrJson<AssignRequest>,
) -> Result<Json<Assignment>, TopologyError> {
    let count = match request.count {
        Some(n) if n > 1 => n as u64,
        _ => 1,
    };
    let writable_volume_count = request.writable_volume_count.unwrap_or_default();
    let option = request.volume_grow_option(&state.options.default_replication)?;

    if !state.topology.has_writable_volume(&option).await {
        if state.topology.free_space() <= 0 {
            debug!("no free volumes");
            return Err(TopologyError::NoFreeSpace("no free volumes".to_string()));
        }
        state
            .volume_grow
            .automatic_grow_by_type(
                &option,
                state.topology.as_ref(),
                writable_volume_count as usize,
            )
            .await?;
    }
    let (fid, count, node) = state.topology.pick_for_write(count, &option).await?;
    let assignment = Assignment {
        fid: FastStr::new(fid.to_string()),
        url: node.url(),
        public_url: node.public_url.clone(),
        count,
        error: FastStr::empty(),
    };
    Ok(Json(assignment))
}

pub async fn lookup_handler(
    State(state): State<DirectoryState>,
    FormOrJson(request): FormOrJson<LookupRequest>,
) -> Result<Json<Lookup>, TopologyError> {
    if request.volume_id.is_empty() {
        return Err(TopologyError::String(
            "volume_id can't be empty".to_string(),
        ));
    }
    let mut volume_id: &str = &request.volume_id;
    if let Some(idx) = volume_id.rfind(',') {
        volume_id = &volume_id[..idx];
    }
    let mut locations = vec![];
    let data_nodes = state
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

            Ok(Json(Lookup::ok(volume_id, locations)))
        }
        None => Err(TopologyError::String(
            "cannot find any locations".to_string(),
        )),
    }
}

pub async fn dir_status_handler(State(state): State<DirectoryState>) -> Json<Topology> {
    let topology = state.topology.topology();
    Json(topology)
}

pub async fn cluster_status_handler(State(state): State<DirectoryState>) -> Json<ClusterStatus> {
    let is_leader = state.topology.is_leader().await;
    let leader = state
        .topology
        .current_leader_address()
        .await
        .unwrap_or_default();
    let peers = state.topology.peers().await;

    let status = ClusterStatus {
        is_leader,
        leader,
        peers,
    };
    Json(status)
}
