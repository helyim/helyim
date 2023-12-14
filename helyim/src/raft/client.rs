use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

use ginepro::LoadBalancedChannel;
use helyim_proto::raft::{raft_server_client::RaftServerClient, Request};
use openraft::error::{ForwardToLeader, NetworkError};
use serde::Serialize;

use crate::{
    raft::types::{
        ClientWriteError, ClientWriteResponse, InitializeError, NodeId, RaftError, RpcError,
    },
    util::parser::parse_addr,
};

pub struct RaftClient {
    /// The leader node to send request to.
    ///
    /// All traffic should be sent to the leader in a cluster.
    pub leader: Arc<Mutex<(NodeId, String)>>,

    pub inner: reqwest::Client,
    pub inner2: RaftServerClient<LoadBalancedChannel>,
}

impl RaftClient {
    /// Create a client with a leader node id and a node manager to get node address by node id.
    pub async fn new(leader_id: NodeId, leader_addr: String) -> Result<Self, RaftError> {
        let channel = LoadBalancedChannel::builder(parse_addr::<RaftError>(&leader_addr)?)
            .channel()
            .await
            .map_err(|err| RaftError::String(err.to_string()))?;
        Ok(Self {
            leader: Arc::new(Mutex::new((leader_id, leader_addr))),
            inner: reqwest::Client::new(),
            inner2: RaftServerClient::new(channel),
        })
    }

    // --- Application API

    /// Submit a write request to the raft cluster.
    ///
    /// The request will be processed by raft protocol: it will be replicated to a quorum and then
    /// will be applied to state machine.
    ///
    /// The result of applying the request will be returned.
    pub async fn set_max_volume_id(
        &mut self,
        max_volume_id: u32,
    ) -> Result<(), RpcError<ClientWriteError>> {
        let mut n_retry = 3;
        loop {
            match self.inner2.set_max_volume_id(max_volume_id).await {
                Ok(_) => return Ok(()),
                Err(status) => {
                    let err: RpcError<ClientWriteError> = bincode_deserialize(status.details())?;
                    if let Some(ForwardToLeader {
                        leader_id: Some(leader_id),
                        leader_node: Some(leader_node),
                    }) = err.forward_to_leader()
                    {
                        {
                            let mut t = self.leader.lock().unwrap();
                            *t = (*leader_id, leader_node.addr.clone());
                        }

                        n_retry -= 1;
                        if n_retry > 0 {
                            continue;
                        }
                    }
                    return Err(err);
                }
            }
        }
    }

    // --- Cluster management API

    /// Initialize a cluster of only the node that receives this request.
    ///
    /// This is the first step to initialize a cluster.
    /// With a initialized cluster, new node can be added with [`write`].
    /// Then setup replication with [`add_learner`].
    /// Then make the new node a member with [`change_membership`].
    pub async fn init(&mut self) -> Result<(), RpcError<InitializeError>> {
        match self.inner2.initialize(()).await {
            Ok(_) => Ok(()),
            Err(status) => Err(bincode_deserialize(status.details())?),
        }
    }

    /// Add a node as learner.
    ///
    /// The node to add has to exist, i.e., being added with `write(ExampleRequest::AddNode{})`
    pub async fn add_learner(
        &mut self,
        req: (NodeId, String),
    ) -> Result<ClientWriteResponse, RpcError<ClientWriteError>> {
        let data = bincode_serialize(&req)?;
        // Retry at most 3 times to find a valid leader.
        let mut n_retry = 3;
        loop {
            match self
                .inner2
                .add_learner(Request { data: data.clone() })
                .await
            {
                Ok(response) => {
                    let response = response.into_inner();
                    return Ok(bincode_deserialize(&response.data)?);
                }
                Err(status) => {
                    let err: RpcError<ClientWriteError> = bincode_deserialize(status.details())?;
                    if let Some(ForwardToLeader {
                        leader_id: Some(leader_id),
                        leader_node: Some(leader_node),
                    }) = err.forward_to_leader()
                    {
                        {
                            let mut t = self.leader.lock().unwrap();
                            *t = (*leader_id, leader_node.addr.clone());
                        }

                        n_retry -= 1;
                        if n_retry > 0 {
                            continue;
                        }
                    }
                    return Err(err);
                }
            }
        }
    }

    /// Change membership to the specified set of nodes.
    ///
    /// All nodes in `req` have to be already added as learner with [`add_learner`],
    /// or an error [`LearnerNotFound`] will be returned.
    pub async fn change_membership(
        &mut self,
        req: &BTreeSet<NodeId>,
    ) -> Result<ClientWriteResponse, RpcError<ClientWriteError>> {
        let data = bincode_serialize(&req)?;
        // Retry at most 3 times to find a valid leader.
        let mut n_retry = 3;
        loop {
            match self
                .inner2
                .change_membership(Request { data: data.clone() })
                .await
            {
                Ok(response) => {
                    let response = response.into_inner();
                    return Ok(bincode_deserialize(&response.data)?);
                }
                Err(status) => {
                    let err: RpcError<ClientWriteError> = bincode_deserialize(status.details())?;
                    if let Some(ForwardToLeader {
                        leader_id: Some(leader_id),
                        leader_node: Some(leader_node),
                    }) = err.forward_to_leader()
                    {
                        {
                            let mut t = self.leader.lock().unwrap();
                            *t = (*leader_id, leader_node.addr.clone());
                        }

                        n_retry -= 1;
                        if n_retry > 0 {
                            continue;
                        }
                    }
                    return Err(err);
                }
            }
        }
    }
}

fn bincode_serialize<T: ?Sized, E>(value: &T) -> Result<Vec<u8>, RpcError<E>>
where
    T: Serialize,
    E: std::error::Error,
{
    bincode::serialize(value).map_err(|err| RpcError::Network(NetworkError::new(&err)))
}

fn bincode_deserialize<'a, T, E>(bytes: &'a [u8]) -> Result<T, RpcError<E>>
where
    T: serde::de::Deserialize<'a>,
    E: std::error::Error,
{
    bincode::deserialize(bytes).map_err(|err| RpcError::Network(NetworkError::new(&err)))
}
