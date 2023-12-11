use std::io::Cursor;

use openraft::{BasicNode, TokioRuntime};

use crate::raft::store::{Request, Response};

pub type NodeId = u64;

openraft::declare_raft_types!(
    /// Declare the type configuration for helyim.
    pub TypeConfig: D = Request, R = Response, NodeId = NodeId, Node = BasicNode,
    Entry = openraft::Entry<TypeConfig>, SnapshotData = Cursor<Vec<u8>>, AsyncRuntime = TokioRuntime
);

pub type Raft = openraft::Raft<TypeConfig>;

// errors

pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<NodeId, E>;
pub type RpcError<E = openraft::error::Infallible> =
    openraft::error::RPCError<NodeId, BasicNode, RaftError<E>>;

pub type ClientWriteError = openraft::error::ClientWriteError<NodeId, BasicNode>;
pub type ForwardToLeader = openraft::error::ForwardToLeader<NodeId, BasicNode>;
pub type InitializeError = openraft::error::InitializeError<NodeId, BasicNode>;

pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<TypeConfig>;
