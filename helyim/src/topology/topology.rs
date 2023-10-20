use std::{sync::Arc, time::Duration};

use dashmap::{mapref::one::RefMut, DashMap};
use faststr::FastStr;
use serde::Serialize;
use tracing::{error, info};

use crate::{
    errors::Result,
    sequence::{Sequence, Sequencer},
    storage::{
        batch_vacuum_volume_check, batch_vacuum_volume_commit, batch_vacuum_volume_compact, FileId,
        VolumeId, VolumeInfo,
    },
    topology::{
        collection::Collection, volume_grow::VolumeGrowOption, DataCenter, DataNodeEventTx,
    },
};

#[derive(Serialize)]
pub struct Topology {
    #[serde(skip)]
    sequencer: Sequencer,
    #[serde(skip)]
    pub collections: Arc<DashMap<FastStr, Collection>>,
    pulse: u64,
    volume_size_limit: u64,
    #[serde(skip)]
    pub data_centers: Arc<DashMap<FastStr, Arc<DataCenter>>>,
    #[serde(skip)]
    shutdown: async_broadcast::Receiver<()>,
}

unsafe impl Send for Topology {}

impl Clone for Topology {
    fn clone(&self) -> Self {
        Self {
            sequencer: self.sequencer.clone(),
            collections: self.collections.clone(),
            pulse: self.pulse,
            volume_size_limit: self.volume_size_limit,
            data_centers: Arc::new(DashMap::new()),
            shutdown: self.shutdown.clone(),
        }
    }
}

impl Topology {
    pub fn new(
        sequencer: Sequencer,
        volume_size_limit: u64,
        pulse: u64,
        shutdown: async_broadcast::Receiver<()>,
    ) -> Topology {
        Topology {
            sequencer,
            collections: Arc::new(DashMap::new()),
            pulse,
            volume_size_limit,
            data_centers: Arc::new(DashMap::new()),
            shutdown,
        }
    }

    pub fn get_or_create_data_center(&self, name: FastStr) -> Arc<DataCenter> {
        self.data_centers
            .entry(name.clone())
            .or_insert_with(|| Arc::new(DataCenter::new(name, self.shutdown.clone())))
            .clone()
    }

    pub fn lookup(&self, collection: FastStr, volume_id: VolumeId) -> Option<Vec<DataNodeEventTx>> {
        if collection.is_empty() {
            for c in self.collections.iter() {
                let data_node = c.lookup(volume_id);
                if data_node.is_some() {
                    return data_node;
                }
            }
        } else if let Some(c) = self.collections.get(&collection) {
            let data_node = c.lookup(volume_id);
            if data_node.is_some() {
                return data_node;
            }
        }

        None
    }

    pub async fn has_writable_volume(&self, option: VolumeGrowOption) -> Result<bool> {
        let collection = self.get_collection(option.collection.clone());
        let vl = collection.get_or_create_volume_layout(option.replica_placement, Some(option.ttl));
        Ok(vl.active_volume_count(option).await? > 0)
    }

    pub async fn free_volumes(&self) -> Result<i64> {
        let mut free = 0;
        for dc_tx in self.data_centers.iter() {
            free += dc_tx.max_volumes().await? - dc_tx.has_volumes().await?;
        }
        Ok(free)
    }

    pub async fn pick_for_write(
        &self,
        count: u64,
        option: VolumeGrowOption,
    ) -> Result<(FileId, u64, DataNodeEventTx)> {
        let (volume_id, nodes) = {
            let collection = self.get_collection(option.collection.clone());
            let layout =
                collection.get_or_create_volume_layout(option.replica_placement, Some(option.ttl));
            layout.pick_for_write(&option).await?
        };

        let file_id = self.sequencer.next_file_id(count)?;

        let file_id = FileId {
            volume_id,
            key: file_id,
            hash: rand::random::<u32>(),
        };
        Ok((file_id, count, nodes[0].clone()))
    }

    pub async fn register_volume_layout(
        &self,
        volume: VolumeInfo,
        data_node: DataNodeEventTx,
    ) -> Result<()> {
        let collection = self.get_collection(volume.collection.clone());
        let mut layout =
            collection.get_or_create_volume_layout(volume.replica_placement, Some(volume.ttl));
        layout.register_volume(&volume, data_node).await
    }

    pub fn unregister_volume_layout(&self, volume: VolumeInfo) {
        let collection = self.get_collection(volume.collection.clone());
        let mut layout =
            collection.get_or_create_volume_layout(volume.replica_placement, Some(volume.ttl));
        layout.unregister_volume(&volume);
    }

    pub async fn next_volume_id(&self) -> Result<VolumeId> {
        let vid = self.get_max_volume_id().await?;

        Ok(vid + 1)
    }

    pub fn set_max_sequence(&self, seq: u64) {
        self.sequencer.set_max(seq);
    }

    pub fn topology(&self) -> Topology {
        self.clone()
    }

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

impl Topology {
    fn get_collection(&self, collection: FastStr) -> RefMut<FastStr, Collection> {
        self.collections
            .entry(collection.clone())
            .or_insert(Collection::new(collection, self.volume_size_limit))
    }

    async fn get_max_volume_id(&self) -> Result<VolumeId> {
        let mut vid = 0;
        for entry in self.data_centers.iter() {
            let other = entry.max_volume_id();
            if other > vid {
                vid = other;
            }
        }

        Ok(vid)
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
