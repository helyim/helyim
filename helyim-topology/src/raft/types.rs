use std::{io::Cursor, net::AddrParseError};

use helyim_common::types::VolumeId;
use openraft::{BasicNode, TokioRuntime, error::InstallSnapshotError};
use serde::{Deserialize, Serialize};

pub type NodeId = u64;

openraft::declare_raft_types!(
    /// Declare the type configuration for helyim.
    pub TypeConfig: D = RaftRequest, R = RaftResponse, NodeId = NodeId, Node = BasicNode,
    Entry = openraft::Entry<TypeConfig>, SnapshotData = Cursor<Vec<u8>>, AsyncRuntime = TokioRuntime
);

pub type Raft = openraft::Raft<TypeConfig>;

// errors

pub type OpenRaftError<E = openraft::error::Infallible> = openraft::error::RaftError<NodeId, E>;
pub type RpcError<E = openraft::error::Infallible> =
    openraft::error::RPCError<NodeId, BasicNode, OpenRaftError<E>>;

pub type ClientWriteError = openraft::error::ClientWriteError<NodeId, BasicNode>;
pub type CheckIsLeaderError = openraft::error::CheckIsLeaderError<NodeId, BasicNode>;
pub type ForwardToLeader = openraft::error::ForwardToLeader<NodeId, BasicNode>;
pub type InitializeError = openraft::error::InitializeError<NodeId, BasicNode>;

pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<TypeConfig>;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum RaftRequest {
    MaxVolumeId { max_volume_id: VolumeId },
}

impl RaftRequest {
    pub fn max_volume_id(max_volume_id: VolumeId) -> Self {
        Self::MaxVolumeId { max_volume_id }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RaftResponse;

#[derive(thiserror::Error, Debug)]
pub enum RaftError {
    #[error("Io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("error: {0}")]
    Box(#[from] Box<dyn std::error::Error + Sync + Send>),
    #[error("Parse integer error: {0}")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("Addr parse error: {0}")]
    AddrParse(#[from] AddrParseError),

    #[error("OpenRaft error: {0}")]
    Raft(#[from] openraft::error::RaftError<NodeId>),
    #[error("OpenRaft rpc error: {0}")]
    Rpc(#[from] openraft::error::RPCError<NodeId, BasicNode, openraft::error::RaftError<NodeId>>),
    #[error("OpenRaft install snapshot error: {0}")]
    InstallSnapshot(#[from] openraft::error::RaftError<NodeId, InstallSnapshotError>),
    #[error("OpenRaft initialize raft cluster error: {0}")]
    InitializeRaftCluster(#[from] openraft::error::RaftError<NodeId, InitializeError>),
    #[error("OpenRaft client write error: {0}")]
    ClientWrite(#[from] openraft::error::RaftError<NodeId, ClientWriteError>),
    #[error("OpenRaft client write error: {0}")]
    CheckIsLeader(#[from] openraft::error::RaftError<NodeId, CheckIsLeaderError>),
    #[error("Openraft fatal error: {0}")]
    Fatal(#[from] openraft::error::Fatal<NodeId>),
}
