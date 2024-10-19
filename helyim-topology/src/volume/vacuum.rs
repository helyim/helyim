use helyim_common::types::VolumeId;
use helyim_proto::volume::{
    VacuumVolumeCheckRequest, VacuumVolumeCleanupRequest, VacuumVolumeCommitRequest,
    VacuumVolumeCompactRequest,
};
use tracing::{error, info};

use crate::{volume_layout::VolumeLayoutRef, DataNodeRef};

pub async fn batch_vacuum_volume_check(
    volume_id: VolumeId,
    data_nodes: &[DataNodeRef],
    garbage_ratio: f64,
) -> bool {
    let mut need_vacuum = true;
    for data_node in data_nodes {
        let request = VacuumVolumeCheckRequest { volume_id };
        let response = data_node.vacuum_volume_check(request).await;
        match response {
            Ok(response) => {
                if response.garbage_ratio > 0.0 {
                    info!(
                        "check volume {}:{volume_id} success. garbage ratio is {}",
                        data_node.public_url, response.garbage_ratio
                    );
                }
                need_vacuum = response.garbage_ratio > garbage_ratio;
            }
            Err(err) => {
                error!(
                    "check volume {}:{volume_id} failed, {err}",
                    data_node.public_url
                );
                need_vacuum = false;
            }
        }
    }
    need_vacuum
}

pub async fn batch_vacuum_volume_compact(
    volume_layout: &VolumeLayoutRef,
    volume_id: VolumeId,
    data_nodes: &[DataNodeRef],
    preallocate: u64,
) -> bool {
    volume_layout.remove_from_writable(&volume_id).await;
    let mut compact_success = true;
    for data_node in data_nodes {
        let request = VacuumVolumeCompactRequest {
            volume_id,
            preallocate,
        };
        let response = data_node.vacuum_volume_compact(request).await;
        match response {
            Ok(_) => {
                info!(
                    "compact volume {}:{volume_id} success.",
                    data_node.public_url
                );
                compact_success = true;
            }
            Err(err) => {
                error!(
                    "compact volume {}:{volume_id} failed, {err}",
                    data_node.public_url
                );
                compact_success = false;
            }
        }
    }
    compact_success
}

pub async fn batch_vacuum_volume_commit(
    volume_layout: &VolumeLayoutRef,
    volume_id: VolumeId,
    data_nodes: &[DataNodeRef],
) -> bool {
    let mut commit_success = true;
    for data_node in data_nodes {
        let request = VacuumVolumeCommitRequest { volume_id };
        match data_node.vacuum_volume_commit(request).await {
            Ok(_) => {
                info!(
                    "commit volume {}:{volume_id} success.",
                    data_node.public_url
                );
            }
            Err(err) => {
                error!(
                    "commit volume {}:{volume_id} failed, {err}",
                    data_node.public_url
                );
                commit_success = false;
            }
        }
    }
    if commit_success {
        for data_node in data_nodes {
            volume_layout
                .set_volume_available(volume_id, data_node)
                .await;
        }
    }

    commit_success
}

pub async fn batch_vacuum_volume_cleanup(volume_id: VolumeId, data_nodes: &[DataNodeRef]) -> bool {
    let mut cleanup_success = true;
    for data_node in data_nodes {
        let request = VacuumVolumeCleanupRequest { volume_id };
        let response = data_node.vacuum_volume_cleanup(request).await;
        match response {
            Ok(_) => {
                info!(
                    "cleanup volume {}:{volume_id} success.",
                    data_node.public_url
                );
                cleanup_success = true;
            }
            Err(_err) => {
                cleanup_success = false;
            }
        }
    }
    cleanup_success
}
