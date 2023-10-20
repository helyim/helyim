use std::{sync::Arc, time::Duration};

use tracing::{error, info};

use crate::{
    errors::Result,
    storage::{batch_vacuum_volume_check, batch_vacuum_volume_commit, batch_vacuum_volume_compact},
    topology::Topology,
};

impl Topology {
    pub async fn vacuum(&self, garbage_threshold: f64, preallocate: u64) -> Result<()> {
        for entry in self.collections.iter() {
            for volume_layout in entry.volume_layouts.iter() {
                for location in volume_layout.locations.iter() {
                    if volume_layout.readonly_volumes.contains(location.key()) {
                        continue;
                    }

                    if batch_vacuum_volume_check(
                        *location.key(),
                        location.value(),
                        garbage_threshold,
                    )
                    .await?
                        && batch_vacuum_volume_compact(
                            volume_layout.value(),
                            *location.key(),
                            location.value(),
                            preallocate,
                        )
                        .await?
                    {
                        batch_vacuum_volume_commit(
                            volume_layout.value(),
                            *location.key(),
                            location.value(),
                        )
                        .await?;
                        // let _ = batch_vacuum_volume_cleanup(vid, data_nodes).await;
                    }
                }
            }
        }

        Ok(())
    }
}

pub async fn topology_vacuum_loop(
    topology: Arc<Topology>,
    garbage_threshold: f64,
    preallocate: u64,
    mut shutdown: async_broadcast::Receiver<()>,
) {
    info!("topology vacuum loop starting");
    let mut interval = tokio::time::interval(Duration::from_secs(60));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                info!("topology vacuum starting.");
                match topology.vacuum(garbage_threshold, preallocate).await {
                    Ok(_) => info!("topology vacuum success."),
                    Err(err) => error!("topology vacuum failed, {err}")
                }
            }
            _ = shutdown.recv() => {
                break;
            }
        }
    }
    info!("topology vacuum loop stopped")
}
