use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use axum::{
    extract::DefaultBodyLimit,
    routing::{get, post},
    Router,
};
use nom::{
    character::complete::{char, digit1},
    sequence::pair,
};
use openraft::{storage::Adaptor, BasicNode, Config};
use tower_http::{compression::CompressionLayer, timeout::TimeoutLayer};
use tracing::{error, info, warn};

use crate::{
    raft::{
        network::{
            api::set_max_volume_id_handler,
            management::{
                add_learner_handler, change_membership_handler, init_handler, metrics_handler,
            },
            raft::{append_handler, snapshot_handler, vote_handler},
            NetworkFactory,
        },
        store::Store,
        types::{
            ClientWriteError, ClientWriteResponse, InitializeError, NodeId, OpenRaftError, Raft,
            RaftError, RaftRequest,
        },
    },
    rt_spawn,
    storage::VolumeId,
    topology::TopologyRef,
    util::exit,
};

mod network;
mod store;
pub mod types;

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
    pub async fn set_topology(&self, topology: &TopologyRef) {
        self.store
            .state_machine
            .write()
            .await
            .set_topology(topology.clone());
    }

    pub async fn initialize(&self) -> Result<(), OpenRaftError<InitializeError>> {
        let mut nodes = BTreeMap::new();
        nodes.insert(
            self.id,
            BasicNode {
                addr: self.addr.clone(),
            },
        );
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

pub fn parse_raft_peer(input: &str) -> Result<(NodeId, &str), RaftError> {
    let (input, (node_id, _)) = pair(digit1, char(':'))(input)?;
    Ok((node_id.parse()?, input))
}

async fn start_raft_node(
    node_id: NodeId,
    http_addr: &str,
    mut shutdown_rx: async_broadcast::Receiver<()>,
) -> Result<RaftServer, RaftError> {
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
    let network = NetworkFactory {};

    // Create a local raft instance.
    let raft = Raft::new(node_id, config.clone(), network, log_store, state_machine)
        .await
        .unwrap();

    let raft_server = RaftServer {
        id: node_id,
        addr: http_addr.to_string(),
        raft,
        store,
        config,
    };

    let raft_state = raft_server.clone();
    let addr = http_addr.parse()?;
    rt_spawn(async move {
        let app = Router::new()
            // application api
            .route("/set_max_volume_id", post(set_max_volume_id_handler))
            // raft management api
            .route("/add-learner", post(add_learner_handler))
            .route("/change-membership", post(change_membership_handler))
            .route("/init", get(init_handler).post(init_handler))
            .route("/metrics", get(metrics_handler).post(metrics_handler))
            // raft communication
            .route("/raft-vote", post(vote_handler))
            .route("/raft-snapshot", post(snapshot_handler))
            .route("/raft-append", post(append_handler))
            .layer((
                CompressionLayer::new(),
                DefaultBodyLimit::max(1024 * 1024 * 50),
                TimeoutLayer::new(Duration::from_secs(10)),
            ))
            .with_state(raft_state);

        match hyper::Server::try_bind(&addr) {
            Ok(builder) => {
                let server = builder.serve(app.into_make_service());
                let graceful = server.with_graceful_shutdown(async {
                    let _ = shutdown_rx.recv().await;
                });
                info!("raft api server starting up. binding addr: {addr}");
                match graceful.await {
                    Ok(()) => info!("raft api server shutting down gracefully."),
                    Err(e) => error!("raft api server stop failed, {}", e),
                }
            }
            Err(err) => {
                error!("starting raft api server failed, error: {err}");
                exit();
            }
        }
    });

    Ok(raft_server)
}

pub async fn start_raft_node_with_peers(
    peers: &[String],
    shutdown_rx: async_broadcast::Receiver<()>,
) -> Result<RaftServer, RaftError> {
    let mut nodes = BTreeMap::new();
    let mut members = BTreeSet::new();

    let (node_id, raft_addr) = parse_raft_peer(&peers[0])?;

    nodes.insert(node_id, BasicNode::new(raft_addr));
    members.insert(node_id);

    let raft_server = start_raft_node(node_id, raft_addr, shutdown_rx).await?;

    // wait for server to startup
    tokio::time::sleep(Duration::from_millis(200)).await;

    if let Err(err) = raft_server.initialize().await {
        warn!("init raft client failed, {err}");
    }

    for peer in peers.iter().skip(1) {
        let (node, host) = parse_raft_peer(peer)?;
        match raft_server.add_learner(node, host).await {
            Ok(_) => {
                members.insert(node);
            }
            Err(err) => warn!("add learner failed, {err}"),
        }
    }

    if let Err(err) = raft_server.change_membership(members).await {
        warn!("change membership failed, {err}");
    }

    Ok(raft_server)
}

#[cfg(test)]
mod tests {
    use crate::raft::{parse_raft_peer, types::RaftRequest};

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
