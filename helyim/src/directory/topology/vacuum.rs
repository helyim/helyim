use helyim_proto::{
    VacuumVolumeCheckRequest, VacuumVolumeCleanupRequest, VacuumVolumeCommitRequest,
    VacuumVolumeCompactRequest,
};
use tracing::{error, info, warn};

use crate::{
    directory::{topology::DataNodeEventTx, Topology, VolumeLayout},
    errors::Result,
    storage::VolumeId,
};

impl Topology {
    pub async fn vacuum(&self, garbage_ratio: f64, preallocate: i64) -> Result<()> {
        for (_name, collection) in self.collections.iter() {
            for (_key, volume_layout) in collection.volume_layouts.iter() {
                let location = volume_layout.locations.clone();
                for (vid, data_nodes) in location {
                    if volume_layout.readonly_volumes.contains(&vid) {
                        continue;
                    }

                    if batch_vacuum_volume_check(vid, &data_nodes, garbage_ratio).await?
                        && batch_vacuum_volume_compact(volume_layout, vid, &data_nodes, preallocate)
                            .await?
                    {
                        batch_vacuum_volume_commit(volume_layout, vid, &data_nodes).await?;
                        // let _ = batch_vacuum_volume_cleanup(vid, data_nodes).await;
                    }
                }
            }
        }

        Ok(())
    }
}

async fn batch_vacuum_volume_check(
    volume_id: VolumeId,
    data_nodes: &[DataNodeEventTx],
    garbage_ratio: f64,
) -> Result<bool> {
    let mut check_success = true;
    for data_node_tx in data_nodes {
        let request = VacuumVolumeCheckRequest { volume_id };
        match data_node_tx.vacuum_volume_check(request).await {
            Ok(response) => {
                info!("check volume {volume_id} success.");
                check_success = response.garbage_ratio > garbage_ratio;
            }
            Err(err) => {
                error!("check volume {volume_id} failed, {err}");
                check_success = false;
            }
        }
    }
    Ok(check_success)
}

async fn batch_vacuum_volume_compact(
    volume_layout: &VolumeLayout,
    volume_id: VolumeId,
    data_nodes: &[DataNodeEventTx],
    preallocate: i64,
) -> Result<bool> {
    volume_layout.remove_from_writable(volume_id);
    let mut compact_success = true;
    for data_node_tx in data_nodes {
        let request = VacuumVolumeCompactRequest {
            volume_id,
            preallocate,
        };
        match data_node_tx.vacuum_volume_compact(request).await {
            Ok(_) => {
                info!("compact volume {volume_id} success.");
                compact_success = true;
            }
            Err(err) => {
                error!("compact volume {volume_id} failed, {err}");
                compact_success = false;
            }
        }
    }
    Ok(compact_success)
}

async fn batch_vacuum_volume_commit(
    volume_layout: &VolumeLayout,
    volume_id: VolumeId,
    data_nodes: &[DataNodeEventTx],
) -> Result<bool> {
    let mut commit_success = true;
    for data_node_tx in data_nodes {
        let request = VacuumVolumeCommitRequest { volume_id };
        match data_node_tx.vacuum_volume_commit(request).await {
            Ok(response) => {
                if response.is_read_only {
                    warn!("volume {volume_id} is read only, will not commit it.");
                    commit_success = false;
                } else {
                    info!("commit volume {volume_id} success.");
                    commit_success = true;
                    volume_layout
                        .set_volume_available(volume_id, data_node_tx)
                        .await?;
                }
            }
            Err(err) => {
                error!("commit volume {volume_id} failed, {err}");
                commit_success = false;
            }
        }
    }
    Ok(commit_success)
}

#[allow(dead_code)]
async fn batch_vacuum_volume_cleanup(
    volume_id: VolumeId,
    data_nodes: &[DataNodeEventTx],
) -> Result<bool> {
    let mut cleanup_success = true;
    for data_node_tx in data_nodes {
        let request = VacuumVolumeCleanupRequest { volume_id };
        match data_node_tx.vacuum_volume_cleanup(request).await {
            Ok(_) => {
                info!("cleanup volume {volume_id} success.");
                cleanup_success = true;
            }
            Err(_err) => {
                cleanup_success = false;
            }
        }
    }
    Ok(cleanup_success)
}
