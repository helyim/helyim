use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
    time::Duration,
};

use axum::{
    extract::DefaultBodyLimit,
    routing::{get, post},
    Router,
};
use faststr::FastStr;
use nom::{
    character::complete::{char, digit1},
    sequence::pair,
};
use openraft::{storage::Adaptor, BasicNode, Config};
use tower_http::{compression::CompressionLayer, timeout::TimeoutLayer};
use tracing::warn;

use crate::{
    raft::{
        client::RaftClient,
        network::{
            api::{max_volume_id_handler, set_max_volume_id_handler},
            management::{
                add_learner_handler, change_membership_handler, init_handler, metrics_handler,
            },
            raft::{append_entries_handler, install_snapshot_handler, vote_handler},
            NetworkFactory,
        },
        store::Store,
        types::{
            ClientWriteError, ClientWriteResponse, InitializeError, NodeId, OpenRaftError, Raft,
            RaftError, RaftRequest,
        },
    },
    storage::VolumeId,
    topology::TopologyRef,
};

pub mod client;
mod network;
mod store;
pub mod types;

fn compute_node_id(addr: &str) -> NodeId {
    let mut node = 0;
    for (idx, c) in addr.char_indices() {
        node += idx * c as usize;
    }
    node as u64
}

fn peer_with_node_id(peers: &[FastStr]) -> HashMap<NodeId, FastStr> {
    let mut peers_with_node_id = HashMap::new();
    for peer in peers {
        let node_id = compute_node_id(peer);
        peers_with_node_id.insert(node_id, peer.clone());
    }
    peers_with_node_id
}

// Representation of an application state. This struct can be shared around to share
// instances of raft, store and more.
#[derive(Clone)]
pub struct RaftServer {
    pub id: NodeId,
    pub addr: String,
    pub raft: Raft,
    pub store: Arc<Store>,
    pub config: Arc<Config>,
}

impl RaftServer {
    pub async fn initialize(
        &self,
        peers: HashMap<NodeId, FastStr>,
    ) -> Result<(), OpenRaftError<InitializeError>> {
        let mut nodes = BTreeMap::new();
        // this raft server should be contained in peers
        for (node, peer) in peers.iter() {
            nodes.insert(
                *node,
                BasicNode {
                    addr: peer.to_string(),
                },
            );
        }
        self.raft.initialize(nodes).await
    }

    pub async fn add_learner(
        &self,
        node_id: NodeId,
        addr: &str,
    ) -> Result<ClientWriteResponse, OpenRaftError<ClientWriteError>> {
        self.raft
            .add_learner(node_id, BasicNode::new(addr), true)
            .await
    }

    pub async fn change_membership(
        &self,
        members: BTreeSet<NodeId>,
    ) -> Result<ClientWriteResponse, OpenRaftError<ClientWriteError>> {
        self.raft.change_membership(members, false).await
    }

    pub async fn set_max_volume_id(
        &self,
        max_volume_id: VolumeId,
    ) -> Result<ClientWriteResponse, OpenRaftError<ClientWriteError>> {
        self.raft
            .client_write(RaftRequest::max_volume_id(max_volume_id))
            .await
    }
}

impl RaftServer {
    pub async fn start_node(node_addr: &str) -> Result<Self, RaftError> {
        // Create a configuration for the raft instance.
        let config = Config {
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            ..Default::default()
        };

        let config = Arc::new(config.validate().unwrap());

        // Create a instance of where the Raft data will be stored.
        let store = Arc::new(Store::default());

        let (log_store, state_machine) = Adaptor::new(store.clone());

        // Create the network layer that will connect and communicate the raft instances and
        // will be used in conjunction with the store created above.
        let network = NetworkFactory::new(node_addr);

        // Create a local raft instance.
        let node_id = compute_node_id(node_addr);
        let raft = Raft::new(node_id, config.clone(), network, log_store, state_machine).await?;

        Ok(RaftServer {
            id: node_id,
            addr: node_addr.to_string(),
            raft,
            store,
            config,
        })
    }

    pub async fn start_node_with_peers(
        &self,
        node_addr: &str,
        peers: &[FastStr],
    ) -> Result<(), RaftError> {
        if node_addr == peers[0] {
            // wait for server to startup
            tokio::time::sleep(Duration::from_millis(200)).await;

            let raft_client = RaftClient::new(node_addr.to_string());

            let peers = peer_with_node_id(peers);
            if let Err(err) = raft_client.init(&peers).await {
                warn!("initialize raft server failed, {err}");
                return Ok(());
            }
        }
        Ok(())
    }

    pub async fn set_topology(&self, topology: &TopologyRef) {
        self.store
            .state_machine
            .write()
            .await
            .set_topology(topology.clone());
    }

    pub async fn current_leader(&self) -> Option<NodeId> {
        self.raft.current_leader().await
    }

    pub async fn current_leader_address(&self) -> Option<FastStr> {
        let current_leader = self.current_leader().await;
        for (node_id, node) in self
            .raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .nodes()
        {
            if current_leader == Some(*node_id) {
                return Some(FastStr::new(&node.addr));
            }
        }
        None
    }

    pub async fn is_leader(&self) -> bool {
        self.current_leader().await == Some(self.id)
    }

    pub fn peers(&self) -> BTreeMap<NodeId, FastStr> {
        let mut map = BTreeMap::new();
        for (node_id, node) in self
            .raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .nodes()
        {
            map.insert(*node_id, FastStr::new(&node.addr));
        }
        map
    }
}

pub fn parse_raft_peer(input: &str) -> Result<(NodeId, &str), RaftError> {
    let (input, (node_id, _)) = pair(digit1, char(':'))(input)?;
    Ok((node_id.parse()?, input))
}

pub fn create_raft_router(raft_state: RaftServer) -> Router {
    Router::new()
        // application api
        .route("/max_volume_id", get(max_volume_id_handler))
        .route("/set_max_volume_id", post(set_max_volume_id_handler))
        // raft management api
        .route("/add-learner", post(add_learner_handler))
        .route("/change-membership", post(change_membership_handler))
        .route("/init", get(init_handler).post(init_handler))
        .route("/metrics", get(metrics_handler).post(metrics_handler))
        // raft communication
        .route("/raft-vote", post(vote_handler))
        .route("/raft-snapshot", post(install_snapshot_handler))
        .route("/raft-append", post(append_entries_handler))
        .layer((
            CompressionLayer::new(),
            DefaultBodyLimit::max(1024 * 1024 * 50),
            TimeoutLayer::new(Duration::from_secs(10)),
        ))
        .with_state(raft_state)
}

#[cfg(test)]
mod tests {
    use crate::raft::{compute_node_id, parse_raft_peer, types::RaftRequest};

    #[test]
    pub fn test_parse_raft_peer() {
        let (node, host) = parse_raft_peer("1:localhost:9333").unwrap();
        assert_eq!(host, "localhost:9333");
        assert_eq!(node, 1);

        let (node, host) = parse_raft_peer("1:127.0.0.1:9333").unwrap();
        assert_eq!(host, "127.0.0.1:9333");
        assert_eq!(node, 1);

        let (node, host) = parse_raft_peer("1:github.com/helyim").unwrap();
        assert_eq!(host, "github.com/helyim");
        assert_eq!(node, 1);

        println!(
            "{}",
            serde_json::to_string(&RaftRequest::max_volume_id(1)).unwrap()
        );
    }
}
