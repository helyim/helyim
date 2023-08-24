use std::collections::HashMap;

use futures::{
    channel::{
        mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    StreamExt,
};
use rand;
use serde::Serialize;

use crate::{
    directory::topology::{
        collection::Collection, data_center::DataCenterEventTx, data_center_loop,
        volume_grow::VolumeGrowOption, volume_layout::VolumeLayout, DataCenter, DataNodeEventTx,
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
    pub data_centers: HashMap<String, DataCenterEventTx>,
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
        }
    }

    pub fn get_or_create_data_center(&mut self, name: &str) -> DataCenterEventTx {
        self.data_centers
            .entry(name.to_string())
            .or_insert_with(|| {
                let (tx, rx) = unbounded();
                rt_spawn(data_center_loop(DataCenter::new(name), rx));
                DataCenterEventTx::new(tx)
            })
            .clone()
    }

    pub fn lookup(&mut self, collection: String, vid: VolumeId) -> Option<Vec<DataNodeEventTx>> {
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

    pub async fn has_writable_volume(&mut self, option: &VolumeGrowOption) -> Result<bool> {
        let vl = self.get_volume_layout(&option.collection, option.replica_placement, option.ttl);

        Ok(vl.active_volume_count(option).await? > 0)
    }

    pub async fn free_volumes(&self) -> Result<i64> {
        let mut free = 0;
        for dc_tx in self.data_centers.values() {
            free += dc_tx.max_volumes().await? - dc_tx.has_volumes().await?;
        }
        Ok(free)
    }

    pub async fn pick_for_write(
        &mut self,
        count: u64,
        option: &VolumeGrowOption,
    ) -> Result<(FileId, u64, DataNodeEventTx)> {
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

    pub async fn register_volume_layout(
        &mut self,
        vi: VolumeInfo,
        dn: DataNodeEventTx,
    ) -> Result<()> {
        self.get_volume_layout(&vi.collection, vi.replica_placement, vi.ttl)
            .register_volume(&vi, dn)
            .await
    }

    pub async fn unregister_volume_layout(&mut self, vi: VolumeInfo) {
        self.get_volume_layout(&vi.collection, vi.replica_placement, vi.ttl)
            .unregister_volume(&vi);
    }

    async fn get_max_volume_id(&self) -> Result<VolumeId> {
        let mut vid = 0;
        for (_, dc_tx) in self.data_centers.iter() {
            let other = dc_tx.max_volume_id().await?;
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

pub enum TopologyEvent {
    GetOrCreateDataCenter(String, oneshot::Sender<DataCenterEventTx>),
    Lookup {
        collection: String,
        volume_id: VolumeId,
        tx: oneshot::Sender<Option<Vec<DataNodeEventTx>>>,
    },
    HasWritableVolume(VolumeGrowOption, oneshot::Sender<Result<bool>>),
    FreeVolumes(oneshot::Sender<Result<i64>>),
    PickForWrite {
        count: u64,
        option: VolumeGrowOption,
        tx: oneshot::Sender<Result<(FileId, u64, DataNodeEventTx)>>,
    },
    RegisterVolumeLayout {
        volume_info: VolumeInfo,
        data_node: DataNodeEventTx,
        tx: oneshot::Sender<Result<()>>,
    },
    UnregisterVolumeLayout(VolumeInfo),
    NextVolumeId(oneshot::Sender<Result<VolumeId>>),
    DataCenters(oneshot::Sender<HashMap<String, DataCenterEventTx>>),
    SetMaxSequence(u64),
    Topology(oneshot::Sender<Topology>),
}

pub async fn topology_loop(
    mut topology: Topology,
    mut topology_rx: UnboundedReceiver<TopologyEvent>,
) {
    while let Some(event) = topology_rx.next().await {
        match event {
            TopologyEvent::GetOrCreateDataCenter(data_center, tx) => {
                let _ = tx.send(topology.get_or_create_data_center(&data_center));
            }
            TopologyEvent::Lookup {
                collection,
                volume_id,
                tx,
            } => {
                let _ = tx.send(topology.lookup(collection, volume_id));
            }
            TopologyEvent::HasWritableVolume(option, tx) => {
                let _ = tx.send(topology.has_writable_volume(&option).await);
            }
            TopologyEvent::FreeVolumes(tx) => {
                let _ = tx.send(topology.free_volumes().await);
            }
            TopologyEvent::PickForWrite { count, option, tx } => {
                let _ = tx.send(topology.pick_for_write(count, &option).await);
            }
            TopologyEvent::RegisterVolumeLayout {
                volume_info,
                data_node,
                tx,
            } => {
                let _ = tx.send(
                    topology
                        .register_volume_layout(volume_info, data_node)
                        .await,
                );
            }
            TopologyEvent::UnregisterVolumeLayout(volume) => {
                topology.unregister_volume_layout(volume).await;
            }
            TopologyEvent::NextVolumeId(tx) => {
                let _ = tx.send(topology.next_volume_id().await);
            }
            TopologyEvent::DataCenters(tx) => {
                let _ = tx.send(topology.data_centers.clone());
            }
            TopologyEvent::SetMaxSequence(seq) => {
                topology.sequence.set_max(seq);
            }
            TopologyEvent::Topology(tx) => {
                let _ = tx.send(topology.clone());
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct TopologyEventTx(UnboundedSender<TopologyEvent>);

impl TopologyEventTx {
    pub fn new(tx: UnboundedSender<TopologyEvent>) -> Self {
        Self(tx)
    }

    pub async fn get_or_create_data_center(
        &self,
        data_center: String,
    ) -> Result<DataCenterEventTx> {
        let (tx, rx) = oneshot::channel();
        self.0
            .unbounded_send(TopologyEvent::GetOrCreateDataCenter(data_center, tx))?;
        Ok(rx.await?)
    }

    pub async fn lookup(
        &self,
        collection: String,
        volume_id: VolumeId,
    ) -> Result<Option<Vec<DataNodeEventTx>>> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(TopologyEvent::Lookup {
            collection,
            volume_id,
            tx,
        })?;
        Ok(rx.await?)
    }

    pub async fn has_writable_volume(&self, option: VolumeGrowOption) -> Result<bool> {
        let (tx, rx) = oneshot::channel();
        self.0
            .unbounded_send(TopologyEvent::HasWritableVolume(option, tx))?;
        rx.await?
    }

    pub async fn free_volumes(&self) -> Result<i64> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(TopologyEvent::FreeVolumes(tx))?;
        rx.await?
    }

    pub async fn pick_for_write(
        &self,
        count: u64,
        option: VolumeGrowOption,
    ) -> Result<(FileId, u64, DataNodeEventTx)> {
        let (tx, rx) = oneshot::channel();
        self.0
            .unbounded_send(TopologyEvent::PickForWrite { count, option, tx })?;
        rx.await?
    }

    pub async fn register_volume_layout(
        &self,
        volume_info: VolumeInfo,
        data_node: DataNodeEventTx,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(TopologyEvent::RegisterVolumeLayout {
            volume_info,
            data_node,
            tx,
        })?;
        rx.await?
    }

    pub fn unregister_volume_layout(&self, volume_info: VolumeInfo) -> Result<()> {
        self.0
            .unbounded_send(TopologyEvent::UnregisterVolumeLayout(volume_info))?;
        Ok(())
    }

    pub async fn next_volume_id(&self) -> Result<VolumeId> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(TopologyEvent::NextVolumeId(tx))?;
        rx.await?
    }

    pub async fn data_centers(&self) -> Result<HashMap<String, DataCenterEventTx>> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(TopologyEvent::DataCenters(tx))?;
        Ok(rx.await?)
    }

    pub fn set_max_sequence(&self, seq: u64) -> Result<()> {
        self.0.unbounded_send(TopologyEvent::SetMaxSequence(seq))?;
        Ok(())
    }

    pub async fn topology(&self) -> Result<Topology> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(TopologyEvent::Topology(tx))?;
        Ok(rx.await?)
    }
}
