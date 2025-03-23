use std::collections::BTreeSet;

use openraft::{
    BasicNode,
    error::{InstallSnapshotError, RemoteError, Unreachable},
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use serde::{Serialize, de::DeserializeOwned};
use tracing::{error, info};

use crate::raft::{
    client::RaftClient,
    types::{ClientWriteResponse, NodeId, RpcError, TypeConfig},
};

#[derive(Clone)]
pub struct NetworkFactory {
    client: RaftClient,
}

impl NetworkFactory {
    pub fn new(addr: &str) -> Self {
        Self {
            client: RaftClient::new(addr.to_string()),
        }
    }

    pub async fn remove_member(&self, member: &NodeId) -> Result<ClientWriteResponse, RpcError> {
        let metrics = self.client.metrics().await?;
        let mut nodes = BTreeSet::new();
        for (node, _) in metrics.membership_config.nodes() {
            if node != member {
                nodes.insert(*node);
            }
        }
        match self.client.change_membership(&nodes).await {
            Ok(response) => {
                info!("remove member {member} success");
                Ok(response)
            }
            Err(err) => {
                error!("remove member {member} failed, error: {err}");
                Err(RpcError::Unreachable(Unreachable::new(&err)))
            }
        }
    }
}

impl NetworkFactory {
    pub async fn send_rpc<Req, Resp, Err>(
        &self,
        target: NodeId,
        target_node: &BasicNode,
        uri: &str,
        req: Req,
    ) -> Result<Resp, openraft::error::RPCError<NodeId, BasicNode, Err>>
    where
        Req: Serialize,
        Err: std::error::Error + DeserializeOwned,
        Resp: DeserializeOwned,
    {
        let url = format!("http://{}/raft/{}", target_node.addr, uri);

        let client = reqwest::Client::new();
        let resp = client
            .post(url)
            .json(&req)
            .send()
            .await
            .map_err(|e| openraft::error::RPCError::Unreachable(Unreachable::new(&e)))?;

        let res: Result<Resp, Err> = resp
            .json()
            .await
            .map_err(|e| openraft::error::RPCError::Unreachable(Unreachable::new(&e)))?;

        res.map_err(|e| openraft::error::RPCError::RemoteError(RemoteError::new(target, e)))
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = NetworkConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        NetworkConnection {
            owner: self.clone(),
            target,
            target_node: node.clone(),
            error_count: 0,
        }
    }
}

pub struct NetworkConnection {
    owner: NetworkFactory,
    target: NodeId,
    target_node: BasicNode,
    error_count: u64,
}

impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RpcError> {
        let append_entries = self
            .owner
            .send_rpc(self.target, &self.target_node, "raft-append", req)
            .await;

        if append_entries.is_err() {
            self.error_count += 1;
            if self.error_count >= 10 {
                self.owner.remove_member(&self.target).await?;
            }
        } else {
            self.error_count = 0;
        }
        append_entries
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, RpcError<InstallSnapshotError>> {
        self.owner
            .send_rpc(self.target, &self.target_node, "raft-snapshot", req)
            .await
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RpcError> {
        self.owner
            .send_rpc(self.target, &self.target_node, "raft-vote", req)
            .await
    }
}
