use async_trait::async_trait;
use openraft::{
    error::{InstallSnapshotError, NetworkError, RemoteError, Unreachable},
    network::{RaftNetwork, RaftNetworkFactory},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    BasicNode,
};
use serde::{de::DeserializeOwned, Serialize};

use crate::raft::types::{NodeId, RpcError, TypeConfig};

pub struct NetworkFactory;

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

        tracing::debug!("client.post() is sent");

        let res: Result<Resp, Err> = resp
            .json()
            .await
            .map_err(|e| openraft::error::RPCError::Network(NetworkError::new(&e)))?;

        res.map_err(|e| openraft::error::RPCError::RemoteError(RemoteError::new(target, e)))
    }
}

#[async_trait]
impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = NetworkConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        NetworkConnection {
            owner: NetworkFactory {},
            target,
            target_node: node.clone(),
        }
    }
}

pub struct NetworkConnection {
    owner: NetworkFactory,
    target: NodeId,
    target_node: BasicNode,
}

/// TODO: when got error, member should be removed from membership
#[async_trait]
impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn send_append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
    ) -> Result<AppendEntriesResponse<NodeId>, RpcError> {
        self.owner
            .send_rpc(self.target, &self.target_node, "raft-append", req)
            .await
    }

    async fn send_install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<InstallSnapshotResponse<NodeId>, RpcError<InstallSnapshotError>> {
        self.owner
            .send_rpc(self.target, &self.target_node, "raft-snapshot", req)
            .await
    }

    async fn send_vote(
        &mut self,
        req: VoteRequest<NodeId>,
    ) -> Result<VoteResponse<NodeId>, RpcError> {
        self.owner
            .send_rpc(self.target, &self.target_node, "raft-vote", req)
            .await
    }
}
