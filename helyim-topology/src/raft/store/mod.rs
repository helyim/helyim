use std::{
    fmt::{Debug, Formatter},
    io::Cursor,
    sync::{Arc, Mutex},
};

use openraft::{
    BasicNode, Entry, EntryPayload, LogId, RaftSnapshotBuilder, RaftTypeConfig, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership,
    storage::{RaftStateMachine, Snapshot},
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info, instrument};

use crate::{
    node::Node,
    raft::types::{NodeId, RaftRequest, RaftResponse, TypeConfig},
    topology::TopologyRef,
};

mod log_store;

pub type LogStore = log_store::LogStore<TypeConfig>;

#[derive(Debug)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, BasicNode>,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

/// Data contained in the Raft state machine. Note that we are using `serde` to serialize the
/// `data`, which has a implementation to be serialized. Note that for this test we set both the key
/// and value as String, but you could set any type of value that has the serialization impl.
#[derive(Serialize, Deserialize, Default, Clone)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogId<NodeId>>,

    pub last_membership: StoredMembership<NodeId, BasicNode>,

    /// Application data.
    #[serde(skip)]
    pub topology: Option<TopologyRef>,
}

impl StateMachineData {
    pub fn set_topology(&mut self, topology: TopologyRef) {
        self.topology = Some(topology);
    }

    pub fn topology(&self) -> &TopologyRef {
        self.topology.as_ref().expect("please initialize topology.")
    }
}

impl Debug for StateMachineData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateMachineData")
            .field("last_applied_log", &self.last_applied_log)
            .field("last_membership", &self.last_membership)
            .finish()
    }
}

/// Defines a state machine for the Raft cluster. This state machine represents a copy of the
/// data for this node. Additionally, it is responsible for storing the last snapshot of the data.
#[derive(Debug, Default)]
pub struct StateMachineStore {
    /// The Raft state machine.
    pub state_machine: RwLock<StateMachineData>,

    snapshot_idx: Arc<Mutex<u64>>,

    /// The last received snapshot.
    current_snapshot: RwLock<Option<StoredSnapshot>>,
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachineStore> {
    #[instrument(level = "trace", skip(self))]
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let data;
        let last_applied_log;
        let last_membership;

        {
            // Serialize the data of the state machine.
            let state_machine = self.state_machine.read().await;
            data = serde_json::to_vec(&*state_machine)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;

            last_applied_log = state_machine.last_applied_log;
            last_membership = state_machine.last_membership.clone();
        }

        let snapshot_idx = {
            let mut l = self.snapshot_idx.lock().unwrap();
            *l += 1;
            *l
        };

        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        {
            let mut current_snapshot = self.current_snapshot.write().await;
            *current_snapshot = Some(snapshot);
        }

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachineStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.last_applied_log,
            state_machine.last_membership.clone(),
        ))
    }

    #[instrument(level = "trace", skip(self, entries))]
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<RaftResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut res = Vec::new(); // No `with_capacity`; do not know `len` of iterator

        let mut sm = self.state_machine.write().await;

        for entry in entries {
            debug!(%entry.log_id, "replicate to sm");

            sm.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => res.push(RaftResponse),
                EntryPayload::Normal(ref req) => match req {
                    RaftRequest::MaxVolumeId { max_volume_id } => {
                        debug!("apply max volume id: {max_volume_id}");
                        sm.topology().adjust_max_volume_id(*max_volume_id).await;
                        res.push(RaftResponse)
                    }
                },
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    res.push(RaftResponse)
                }
            };
        }
        Ok(res)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    #[instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<<TypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    #[instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        info!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );

        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        // Update the state machine.
        {
            let updated_state_machine: StateMachineData =
                serde_json::from_slice(&new_snapshot.data).map_err(|e| {
                    StorageIOError::read_snapshot(Some(new_snapshot.meta.signature()), &e)
                })?;
            let mut state_machine = self.state_machine.write().await;
            *state_machine = updated_state_machine;
        }

        // Update current snapshot.
        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(new_snapshot);
        Ok(())
    }

    #[instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => Ok(None),
        }
    }
}
