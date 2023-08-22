use std::{collections::HashMap, sync::Arc};

use futures::{
    channel::{
        mpsc::{unbounded, UnboundedSender},
        oneshot,
    },
    lock::Mutex,
};
use rand;
use serde::Serialize;

use crate::{
    directory::topology::{
        collection::Collection, data_center::DataCenterEvent, data_center_loop,
        volume_grow::VolumeGrowOption, volume_layout::VolumeLayout, DataCenter, DataNode,
    },
    errors::Result,
    rt_spawn,
    sequence::{MemorySequencer, Sequencer},
    storage::{FileId, ReplicaPlacement, Ttl, VolumeId, VolumeInfo},
};

#[derive(Debug, Clone, Serialize)]
pub struct Topology {
    #[serde(skip)]
    pub sequence: MemorySequencer,
    pub collections: HashMap<String, Collection>,
    pub pulse: u64,
    pub volume_size_limit: u64,
    #[serde(skip)]
    pub data_centers: HashMap<String, Arc<Mutex<DataCenter>>>,
    #[serde(skip)]
    pub data_centers_tx: HashMap<String, UnboundedSender<DataCenterEvent>>,
}

unsafe impl Send for Topology {}

impl Topology {
    pub fn new(sequence: MemorySequencer, volume_size_limit: u64, pulse: u64) -> Topology {
        Topology {
            sequence,
            collections: HashMap::new(),
            pulse,
            volume_size_limit,
            data_centers: HashMap::new(),
            data_centers_tx: HashMap::new(),
        }
    }

    #[deprecated]
    pub fn get_or_create_data_center(&mut self, name: &str) -> Arc<Mutex<DataCenter>> {
        self.data_centers
            .entry(name.to_string())
            .or_insert(Arc::new(Mutex::new(DataCenter::new(name))))
            .clone()
    }

    pub fn get_or_create_data_center_tx(&mut self, name: &str) -> UnboundedSender<DataCenterEvent> {
        self.data_centers_tx
            .entry(name.to_string())
            .or_insert_with(|| {
                let (tx, rx) = unbounded();
                rt_spawn(data_center_loop(DataCenter::new(name), rx));
                tx
            })
            .clone()
    }

    pub fn lookup(
        &mut self,
        collection: String,
        vid: VolumeId,
    ) -> Option<Vec<Arc<Mutex<DataNode>>>> {
        if collection.is_empty() {
            for c in self.collections.values() {
                let data_node = c.lookup(vid);
                if data_node.is_some() {
                    return data_node;
                }
            }
        } else if let Some(c) = self.collections.get(&collection) {
            let data_node = c.lookup(vid);
            if data_node.is_some() {
                return data_node;
            }
        }

        None
    }

    fn get_volume_layout(
        &mut self,
        collection: &str,
        rp: ReplicaPlacement,
        ttl: Ttl,
    ) -> &mut VolumeLayout {
        self.collections
            .entry(collection.to_string())
            .or_insert(Collection::new(collection, self.volume_size_limit))
            .get_or_create_volume_layout(rp, Some(ttl))
    }

    pub async fn has_writable_volume(&mut self, option: &VolumeGrowOption) -> bool {
        let vl = self.get_volume_layout(&option.collection, option.replica_placement, option.ttl);

        vl.active_volume_count(option).await > 0
    }

    pub async fn free_volumes(&self) -> Result<i64> {
        let mut free = 0;
        for dc_tx in self.data_centers_tx.values() {
            let (tx, max_volumes_rx) = oneshot::channel();
            dc_tx.unbounded_send(DataCenterEvent::MaxVolumes(tx))?;

            let (tx, has_volumes_rx) = oneshot::channel();
            dc_tx.unbounded_send(DataCenterEvent::HasVolumes(tx))?;

            free += max_volumes_rx.await?? - has_volumes_rx.await??;
        }
        Ok(free)
    }

    #[deprecated]
    pub async fn pick_for_write(
        &mut self,
        count: u64,
        option: &VolumeGrowOption,
    ) -> Result<(FileId, u64, Arc<Mutex<DataNode>>)> {
        let (volume_id, nodes) = {
            let layout =
                self.get_volume_layout(&option.collection, option.replica_placement, option.ttl);
            layout.pick_for_write(option).await?
        };

        let (file_id, count) = self.sequence.next_file_id(count);

        let file_id = FileId {
            volume_id,
            key: file_id,
            hash_code: rand::random::<u32>(),
        };
        Ok((file_id, count, nodes[0].clone()))
    }

    pub async fn register_volume_layout(&mut self, vi: VolumeInfo, dn: Arc<Mutex<DataNode>>) {
        self.get_volume_layout(&vi.collection, vi.replica_placement, vi.ttl)
            .register_volume(&vi, dn)
            .await;
    }

    pub async fn unregister_volume_layout(&mut self, vi: VolumeInfo, dn: Arc<Mutex<DataNode>>) {
        self.get_volume_layout(&vi.collection, vi.replica_placement, vi.ttl)
            .unregister_volume(&vi, dn);
    }

    pub async fn get_max_volume_id(&self) -> Result<VolumeId> {
        let mut vid = 0;
        for (_, dc_tx) in self.data_centers_tx.iter() {
            let (tx, rx) = oneshot::channel();
            dc_tx.unbounded_send(DataCenterEvent::MaxVolumeId(tx))?;
            let other = rx.await?;
            if other > vid {
                vid = other;
            }
        }

        Ok(vid)
    }

    pub async fn next_volume_id(&mut self) -> Result<VolumeId> {
        let vid = self.get_max_volume_id().await?;

        Ok(vid + 1)
    }
}
