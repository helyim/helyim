use std::io::Cursor;

use openraft::{BasicNode, TokioRuntime};
use serde::{Deserialize, Serialize};

use crate::storage::VolumeId;

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

/// Here you will set the types of request that will interact with the raft nodes.
/// For example the `Set` will be used to write data (key and value) to the raft database.
/// The `AddNode` will append a new node to the current existing shared list of nodes.
/// You will want to add any request that can write data in all nodes here.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    MaxVolumeId(VolumeId),
}

/// Here you will defined what type of answer you expect from reading the data of a node.
/// In this example it will return a optional value from a given key in
/// the `Request.Set`.
///
/// TODO: Should we explain how to create multiple `AppDataResponse`?
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Response {
    pub value: Option<String>,
}
