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
use bytes::Bytes;
use helyim_proto::raft::{Request as RaftRequest, Response as RaftResponse};
use nom::{
    character::complete::{char, digit1},
    sequence::pair,
};
use openraft::{
    raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest},
    storage::Adaptor,
    BasicNode, Config,
};
use tonic::{Code, Request, Response, Status};
use tower_http::{compression::CompressionLayer, timeout::TimeoutLayer};
use tracing::{error, info, warn};

use crate::{
    raft::{
        client::RaftClient,
        network::{
            api::{read_handler, write_handler},
            management::{
                add_learner_handler, change_membership_handler, init_handler, metrics_handler,
            },
            raft::{append_handler, snapshot_handler, vote_handler},
            Network,
        },
        store::Store,
        types::{NodeId, Raft, RaftError, TypeConfig},
    },
    rt_spawn,
    topology::TopologyRef,
    util::exit,
};

pub mod client;
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

#[tonic::async_trait]
impl helyim_proto::raft::raft_server_server::RaftServer for RaftServer {
    async fn initialize(&self, _request: Request<()>) -> Result<Response<()>, Status> {
        let mut nodes = BTreeMap::new();
        nodes.insert(
            self.id,
            BasicNode {
                addr: self.addr.clone(),
            },
        );
        match self.raft.initialize(nodes).await {
            Ok(_) => Ok(Response::new(())),
            Err(err) => Err(Status::with_details(
                Code::Internal,
                "initialize failed",
                Bytes::from(bincode_serialize(&err)?),
            )),
        }
    }

    async fn add_learner(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<Response<RaftResponse>, Status> {
        let request = request.into_inner();
        let (node_id, addr) = bincode_deserialize(&request.data)?;
        let node = BasicNode { addr };
        match self.raft.add_learner(node_id, node, true).await {
            Ok(client_write) => Ok(Response::new(RaftResponse {
                data: bincode_serialize(&client_write)?,
            })),
            Err(err) => Err(Status::with_details(
                Code::Internal,
                "add learner failed",
                Bytes::from(bincode_serialize(&err)?),
            )),
        }
    }

    async fn change_membership(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<Response<RaftResponse>, Status> {
        let request = request.into_inner();
        let members: BTreeSet<NodeId> = bincode_deserialize(&request.data)?;
        match self.raft.change_membership(members, false).await {
            Ok(client_write) => Ok(Response::new(RaftResponse {
                data: bincode_serialize(&client_write)?,
            })),
            Err(err) => Err(Status::with_details(
                Code::Internal,
                "change membership failed",
                Bytes::from(bincode_serialize(&err)?),
            )),
        }
    }

    async fn vote(&self, request: Request<RaftRequest>) -> Result<Response<RaftResponse>, Status> {
        let request = request.into_inner();
        let vote: VoteRequest<NodeId> = bincode_deserialize(&request.data)?;
        match self.raft.vote(vote).await {
            Ok(vote) => Ok(Response::new(RaftResponse {
                data: bincode_serialize(&vote)?,
            })),
            Err(err) => Err(Status::with_details(
                Code::Internal,
                "vote failed",
                Bytes::from(bincode_serialize(&err)?),
            )),
        }
    }

    async fn append_entries(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<Response<RaftResponse>, Status> {
        let request = request.into_inner();
        let append_entries: AppendEntriesRequest<TypeConfig> = bincode_deserialize(&request.data)?;
        match self.raft.append_entries(append_entries).await {
            Ok(append_entries) => Ok(Response::new(RaftResponse {
                data: bincode_serialize(&append_entries)?,
            })),
            Err(err) => Err(Status::with_details(
                Code::Internal,
                "append entries failed",
                Bytes::from(bincode_serialize(&err)?),
            )),
        }
    }

    async fn install_snapshot(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<Response<RaftResponse>, Status> {
        let request = request.into_inner();
        let install_snapshot: InstallSnapshotRequest<TypeConfig> =
            bincode_deserialize(&request.data)?;
        match self.raft.install_snapshot(install_snapshot).await {
            Ok(install_snapshot) => Ok(Response::new(RaftResponse {
                data: bincode_serialize(&install_snapshot)?,
            })),
            Err(err) => Err(Status::with_details(
                Code::Internal,
                "append entries failed",
                Bytes::from(bincode_serialize(&err)?),
            )),
        }
    }

    async fn set_max_volume_id(&self, request: Request<u32>) -> Result<Response<()>, Status> {
        let max_volume_id = request.into_inner();
        if let Some(topology) = self.store.state_machine.read().await.topology.as_ref() {
            topology
                .write()
                .await
                .adjust_max_volume_id(max_volume_id)
                .await;
        }
        Ok(Response::new(()))
    }

    async fn max_volume_id(&self, _request: Request<()>) -> Result<Response<u32>, Status> {
        self.raft
            .ensure_linearizable()
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        match self.store.state_machine.read().await.topology.as_ref() {
            Some(topology) => {
                let max_volume_id = topology.read().await.max_volume_id();
                Ok(Response::new(max_volume_id))
            }
            None => Err(Status::internal("topology is not initialized.")),
        }
    }
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

pub fn parse_raft_peer(input: &str) -> Result<(NodeId, &str), RaftError> {
    let (input, (node_id, _)) = pair(digit1, char(':'))(input)?;
    Ok((node_id.parse()?, input))
}

fn bincode_serialize<T: ?Sized>(value: &T) -> Result<Vec<u8>, Status>
where
    T: serde::Serialize,
{
    bincode::serialize(value).map_err(|err| Status::internal(err.to_string()))
}

fn bincode_deserialize<'a, T>(bytes: &'a [u8]) -> Result<T, Status>
where
    T: serde::de::Deserialize<'a>,
{
    bincode::deserialize(bytes).map_err(|err| Status::internal(err.to_string()))
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

    let raft_state = raft_server.clone();
    let addr = http_addr.parse()?;
    rt_spawn(async move {
        let app = Router::new()
            // application api
            .route("/write", post(write_handler))
            .route("/read", get(read_handler).post(read_handler))
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
) -> Result<(RaftServer, RaftClient), RaftError> {
    let mut nodes = BTreeMap::new();
    let mut members = BTreeSet::new();

    let (node_id, raft_addr) = parse_raft_peer(&peers[0])?;

    nodes.insert(node_id, BasicNode::new(raft_addr));
    members.insert(node_id);

    let raft_server = start_raft_node(node_id, raft_addr, shutdown_rx).await?;

    // wait for server to startup
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut client = RaftClient::new(node_id, raft_addr.to_string()).await?;
    if let Err(err) = client.init().await {
        warn!("init raft client failed, {err}");
    }

    for peer in peers.iter().skip(1) {
        let (node, host) = parse_raft_peer(peer)?;
        members.insert(node);
        if let Err(err) = client.add_learner((node, host.to_string())).await {
            warn!("add learner failed, {err}");
        }
    }

    if let Err(err) = client.change_membership(&members).await {
        warn!("change membership failed, {err}");
    }

    Ok((raft_server, client))
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
