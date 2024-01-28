use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};

use faststr::FastStr;
use openraft::{
    error::{ForwardToLeader, NetworkError, RemoteError},
    BasicNode, RaftMetrics, TryAsRef,
};
use serde::{de::DeserializeOwned, Serialize};
use tokio::time::timeout;
use tracing::{debug, error, info};

use crate::{
    raft::types::{
        self, ClientWriteError, ClientWriteResponse, InitializeError, NodeId, OpenRaftError,
        RaftRequest, RpcError,
    },
    storage::VolumeId,
};

#[derive(Clone)]
pub struct RaftClient {
    /// The leader node to send request to.
    ///
    /// All traffic should be sent to the leader in a cluster.
    pub leader: Arc<Mutex<(Option<NodeId>, String)>>,

    pub inner: reqwest::Client,
}

impl RaftClient {
    /// Create a client with a leader node id and a node manager to get node address by node id.
    pub fn new(leader_addr: String) -> Self {
        Self {
            leader: Arc::new(Mutex::new((None, leader_addr))),
            inner: reqwest::Client::new(),
        }
    }

    // --- Application API

    /// Submit a write request to the raft cluster.
    ///
    /// The request will be processed by raft protocol: it will be replicated to a quorum and then
    /// will be applied to state machine.
    ///
    /// The result of applying the request will be returned.
    pub async fn set_max_volume_id(
        &self,
        max_volume_id: VolumeId,
    ) -> Result<ClientWriteResponse, RpcError<ClientWriteError>> {
        self.send_rpc_to_leader(
            "set_max_volume_id",
            Some(&RaftRequest::max_volume_id(max_volume_id)),
        )
        .await
    }

    // --- Cluster management API

    /// Initialize a cluster of only the node that receives this request.
    ///
    /// This is the first step to initialize a cluster.
    /// With a initialized cluster, new node can be added with [`write`].
    /// Then setup replication with [`add_learner`].
    /// Then make the new node a member with [`change_membership`].
    pub async fn init(
        &self,
        req: &HashMap<NodeId, FastStr>,
    ) -> Result<(), RpcError<InitializeError>> {
        self.do_send_rpc_to_leader("init", Some(req)).await
    }

    /// Add a node as learner.
    pub async fn add_learner(
        &self,
        req: (NodeId, String),
    ) -> Result<ClientWriteResponse, RpcError<ClientWriteError>> {
        self.send_rpc_to_leader("add-learner", Some(&req)).await
    }

    /// Change membership to the specified set of nodes.
    ///
    /// All nodes in `req` have to be already added as learner with [`add_learner`],
    /// or an error [`LearnerNotFound`] will be returned.
    pub async fn change_membership(
        &self,
        req: &BTreeSet<NodeId>,
    ) -> Result<ClientWriteResponse, RpcError<ClientWriteError>> {
        self.send_rpc_to_leader("change-membership", Some(req))
            .await
    }

    /// Get the metrics about the cluster.
    ///
    /// Metrics contains various information about the cluster, such as current leader,
    /// membership config, replication status etc.
    /// See [`RaftMetrics`].
    pub async fn metrics(&self) -> Result<RaftMetrics<NodeId, BasicNode>, RpcError> {
        self.do_send_rpc_to_leader("metrics", None::<&()>).await
    }

    // --- Internal methods

    /// Send RPC to specified node.
    ///
    /// It sends out a POST request if `req` is Some. Otherwise a GET request.
    /// The remote endpoint must respond a reply in form of `Result<T, E>`.
    /// An `Err` happened on remote will be wrapped in an
    /// [`openraft::error::RPCError::RemoteError`].
    async fn do_send_rpc_to_leader<Req, Resp, Err>(
        &self,
        uri: &str,
        req: Option<&Req>,
    ) -> Result<Resp, RpcError<Err>>
    where
        Req: Serialize + 'static,
        Resp: Serialize + DeserializeOwned,
        Err: std::error::Error + Serialize + DeserializeOwned,
    {
        let (leader_id, url) = {
            let t = self.leader.lock().unwrap();
            let target_addr = &t.1;
            (
                t.0.unwrap_or_default(),
                format!("http://{}/raft/{}", target_addr, uri),
            )
        };

        let fut = if let Some(r) = req {
            debug!(
                ">>> client send request to {}: {}",
                url,
                serde_json::to_string_pretty(&r).unwrap()
            );
            self.inner.post(&url).json(r)
        } else {
            debug!(">>> client send request to {}", url);
            self.inner.get(&url)
        }
        .send();

        let timeout_fut = timeout(Duration::from_millis(3_000), fut).await;
        let response = match timeout_fut {
            Ok(x) => x.map_err(|e| RpcError::Network(NetworkError::new(&e)))?,
            Err(err) => {
                error!("timeout {} to url: {}", err, url);
                return Err(RpcError::Network(NetworkError::new(&err)));
            }
        };

        let response: Result<Resp, OpenRaftError<Err>> = response
            .json()
            .await
            .map_err(|e| RpcError::Network(NetworkError::new(&e)))?;
        debug!(
            "<<< client recv reply from {}: {}",
            url,
            serde_json::to_string_pretty(&response).unwrap()
        );

        response.map_err(|e| {
            if leader_id == 0 {
                debug!("leader is unknown, replace it with 0");
            }
            RpcError::RemoteError(RemoteError::new(leader_id, e))
        })
    }

    /// Try the best to send a request to the leader.
    ///
    /// If the target node is not a leader, a `ForwardToLeader` error will be
    /// returned and this client will retry at most 3 times to contact the updated leader.
    async fn send_rpc_to_leader<Req, Resp, Err>(
        &self,
        uri: &str,
        req: Option<&Req>,
    ) -> Result<Resp, RpcError<Err>>
    where
        Req: Serialize + 'static,
        Resp: Serialize + DeserializeOwned,
        Err: std::error::Error
            + Serialize
            + DeserializeOwned
            + TryAsRef<types::ForwardToLeader>
            + Clone,
    {
        // Retry at most 3 times to find a valid leader.
        let mut n_retry = 3;

        loop {
            let res: Result<Resp, RpcError<Err>> = self.do_send_rpc_to_leader(uri, req).await;

            let rpc_err = match res {
                Ok(x) => return Ok(x),
                Err(rpc_err) => rpc_err,
            };

            if let Some(ForwardToLeader {
                leader_id,
                leader_node: Some(leader_node),
            }) = rpc_err.forward_to_leader()
            {
                // Update target to the new leader.
                {
                    let mut t = self.leader.lock().unwrap();
                    info!(
                        "leader changed, current leader -> node id: {leader_id:?}, address: {}",
                        leader_node.addr
                    );
                    *t = (*leader_id, leader_node.addr.clone());
                }

                n_retry -= 1;
                if n_retry > 0 {
                    continue;
                }
            }

            return Err(rpc_err);
        }
    }
}
