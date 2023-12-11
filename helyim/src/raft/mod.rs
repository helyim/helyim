use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    io::Cursor,
    sync::Arc,
    time::Duration,
};

use actix_web::{middleware, middleware::Logger, rt::System, web::Data, HttpServer};
use once_cell::sync::Lazy;
use openraft::{storage::Adaptor, BasicNode, Config, TokioRuntime};
use parking_lot::RwLock;

use crate::{
    errors::{Error, Result},
    raft::{
        client::RaftClient,
        network::{api, management, raft, Network},
        store::{Request, Response, Store},
    },
    topology::TopologyRef,
    util::parser::parse_raft_peer,
};

pub mod client;
pub mod network;
pub mod store;

pub type NodeId = u64;

pub static RAFT_NODE_MAPPER: Lazy<RwLock<HashMap<NodeId, String>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

openraft::declare_raft_types!(
    /// Declare the type configuration for helyim.
    pub TypeConfig: D = Request, R = Response, NodeId = NodeId, Node = BasicNode,
    Entry = openraft::Entry<TypeConfig>, SnapshotData = Cursor<Vec<u8>>, AsyncRuntime = TokioRuntime
);

pub type LogStore = Adaptor<TypeConfig, Arc<Store>>;
pub type StateMachineStore = Adaptor<TypeConfig, Arc<Store>>;
pub type Raft = openraft::Raft<TypeConfig>;

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
    pub async fn set_topology(self, topology: &TopologyRef) {
        self.store
            .state_machine
            .write()
            .await
            .set_topology(topology.clone());
    }
}

pub mod typ {
    use openraft::BasicNode;

    use crate::raft::{NodeId, TypeConfig};

    pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<NodeId, E>;
    pub type RpcError<E = openraft::error::Infallible> =
        openraft::error::RPCError<NodeId, BasicNode, RaftError<E>>;

    pub type ClientWriteError = openraft::error::ClientWriteError<NodeId, BasicNode>;
    pub type CheckIsLeaderError = openraft::error::CheckIsLeaderError<NodeId, BasicNode>;
    pub type ForwardToLeader = openraft::error::ForwardToLeader<NodeId, BasicNode>;
    pub type InitializeError = openraft::error::InitializeError<NodeId, BasicNode>;

    pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<TypeConfig>;
}

async fn start_raft_node(node_id: NodeId, http_addr: &str) -> std::io::Result<RaftServer> {
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
    let network = Network {};

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

    // Create an application that will store all the instances created above, this will
    // be later used on the actix-web services.
    let app_data = Data::new(raft_server.clone());

    // Start the actix-web server.
    let server = HttpServer::new(move || {
        actix_web::App::new()
            .wrap(Logger::default())
            .wrap(Logger::new("%a %{User-Agent}i"))
            .wrap(middleware::Compress::default())
            .app_data(app_data.clone())
            // raft internal RPC
            .service(raft::append)
            .service(raft::snapshot)
            .service(raft::vote)
            // admin API
            .service(management::init)
            .service(management::add_learner)
            .service(management::change_membership)
            .service(management::metrics)
            // application API
            .service(api::write)
            .service(api::read)
    });
    let server = server.bind(http_addr)?.run();

    // stop the server
    // let server_handle = server.handle();
    // server_handle.stop(true).await;

    std::thread::spawn(move || System::new().block_on(server));

    Ok(raft_server)
}

pub async fn start_raft_node_with_peers(peers: &[String]) -> Result<(RaftServer, RaftClient)> {
    let mut nodes = BTreeMap::new();
    let mut members = BTreeSet::new();

    let (node_id, raft_addr) = parse_raft_peer(&peers[0])?;

    nodes.insert(node_id, BasicNode::new(raft_addr));
    members.insert(node_id);

    let raft_server = start_raft_node(node_id, raft_addr).await?;

    // wait for server to startup
    tokio::time::sleep(Duration::from_millis(200)).await;

    let client = RaftClient::new(node_id, raft_addr.to_string());
    client.init().await.map_err(|err| Error::Box(err.into()))?;

    for peer in peers.iter().skip(1) {
        let (node, host) = parse_raft_peer(peer)?;
        members.insert(node);
        client
            .add_learner((node, host.to_string()))
            .await
            .map_err(|err| Error::Box(err.into()))?;
    }

    let _ = client
        .change_membership(&members)
        .await
        .map_err(|err| Error::Box(err.into()))?;

    Ok((raft_server, client))
}
