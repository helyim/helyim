use std::{
    net::SocketAddr, num::ParseIntError, pin::Pin, result::Result as StdResult, sync::Arc,
    time::Duration,
};

use axum::{extract::DefaultBodyLimit, routing::get, Router};
use faststr::FastStr;
use futures::{Stream, StreamExt};
use helyim_proto::{
    helyim_server::{Helyim, HelyimServer},
    lookup_volume_response::VolumeLocation,
    HeartbeatRequest, HeartbeatResponse, Location, LookupEcVolumeRequest, LookupEcVolumeResponse,
    LookupVolumeRequest, LookupVolumeResponse,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{transport::Server as TonicServer, Request, Response, Status, Streaming};
use tower_http::{compression::CompressionLayer, timeout::TimeoutLayer};
use tracing::{debug, error, info};

use crate::{
    directory::api::{
        assign_handler, cluster_status_handler, dir_status_handler, lookup_handler,
        DirectoryContext,
    },
    errors::Result,
    raft::{create_raft_router, RaftServer},
    rt_spawn,
    sequence::Sequencer,
    storage::VolumeInfo,
    topology::{topology_vacuum_loop, volume_grow::VolumeGrowth, TopologyRef},
    util::{args::MasterOptions, exit, get_or_default, http::default_handler},
};

pub struct DirectoryServer {
    pub host: FastStr,
    pub options: Arc<MasterOptions>,
    pub garbage_threshold: f64,
    pub topology: TopologyRef,
    pub volume_grow: Arc<VolumeGrowth>,

    shutdown: async_broadcast::Sender<()>,
}

impl DirectoryServer {
    pub async fn new(
        host: &str,
        options: MasterOptions,
        garbage_threshold: f64,
        sequencer: Sequencer,
    ) -> Result<DirectoryServer> {
        let master_opts = Arc::new(options);

        let (shutdown, mut shutdown_rx) = async_broadcast::broadcast(16);
        let volume_size_limit_mb = master_opts.volume_size_limit_mb;

        let topology = TopologyRef::new(
            sequencer,
            volume_size_limit_mb * 1024 * 1024,
            master_opts.pulse_seconds,
        );

        rt_spawn(topology_vacuum_loop(
            topology.clone(),
            garbage_threshold,
            volume_size_limit_mb * (1 << 20),
            shutdown_rx.clone(),
        ));

        let master = DirectoryServer {
            host: FastStr::new(host),
            options: master_opts,
            garbage_threshold,
            volume_grow: Arc::new(VolumeGrowth),
            topology: topology.clone(),
            shutdown,
        };

        let addr = format!("{}:{}", host, master.options.port + 1).parse()?;
        rt_spawn(async move {
            info!("directory grpc server starting up. binding addr: {addr}");
            if let Err(err) = TonicServer::builder()
                .add_service(HelyimServer::new(DirectoryGrpcServer {
                    volume_size_limit_mb,
                    topology,
                }))
                .serve_with_shutdown(addr, async {
                    let _ = shutdown_rx.recv().await;
                })
                .await
            {
                error!("directory grpc server starting failed, {err}");
                exit();
            }
        });

        Ok(master)
    }

    pub async fn stop(&mut self) -> Result<()> {
        self.shutdown.broadcast(()).await?;
        Ok(())
    }

    pub async fn start(&mut self) -> Result<()> {
        let raft_node_id = self.options.raft.node_id;
        let raft_node_addr = format!("{}:{}", self.options.ip, self.options.port);
        // start raft node and control cluster with `raft_client`
        let raft_server = RaftServer::start_node(raft_node_id, &raft_node_addr).await?;
        raft_server.set_topology(&self.topology.clone()).await;
        self.topology
            .write()
            .await
            .set_raft_server(raft_server.clone());

        // http server
        let ctx = DirectoryContext {
            topology: self.topology.clone(),
            volume_grow: self.volume_grow.clone(),
            options: self.options.clone(),
        };
        let addr = format!("{}:{}", self.host, self.options.port).parse()?;
        let shutdown_rx = self.shutdown.new_receiver();
        let raft_router = create_raft_router(raft_server.clone());

        rt_spawn(start_directory_server(ctx, addr, shutdown_rx, raft_router));

        raft_server
            .start_node_with_peer(
                raft_node_id,
                &raft_node_addr,
                self.options.raft.peer.as_ref(),
            )
            .await?;

        Ok(())
    }
}

async fn start_directory_server(
    ctx: DirectoryContext,
    addr: SocketAddr,
    mut shutdown: async_broadcast::Receiver<()>,
    raft_router: Router,
) {
    let http_router = Router::new()
        .route("/dir/assign", get(assign_handler).post(assign_handler))
        .route("/dir/lookup", get(lookup_handler).post(lookup_handler))
        .route(
            "/dir/status",
            get(dir_status_handler).post(dir_status_handler),
        )
        .route(
            "/cluster/status",
            get(cluster_status_handler).post(cluster_status_handler),
        )
        .fallback(default_handler)
        .layer((
            CompressionLayer::new(),
            DefaultBodyLimit::max(1024 * 1024),
            TimeoutLayer::new(Duration::from_secs(10)),
        ))
        .with_state(ctx);

    let app = http_router.merge(Router::new().nest("/raft", raft_router));

    match hyper::Server::try_bind(&addr) {
        Ok(builder) => {
            let server = builder.serve(app.into_make_service());
            let graceful = server.with_graceful_shutdown(async {
                let _ = shutdown.recv().await;
            });
            info!("directory api server starting up. binding addr: {addr}");
            match graceful.await {
                Ok(()) => info!("directory api server shutting down gracefully."),
                Err(e) => error!("directory api server stop failed, {}", e),
            }
        }
        Err(err) => {
            error!("starting directory api server failed, error: {err}");
            exit();
        }
    }
}

#[derive(Clone)]
struct DirectoryGrpcServer {
    pub volume_size_limit_mb: u64,
    pub topology: TopologyRef,
}

#[tonic::async_trait]
impl Helyim for DirectoryGrpcServer {
    type HeartbeatStream = Pin<Box<dyn Stream<Item = StdResult<HeartbeatResponse, Status>> + Send>>;

    async fn heartbeat(
        &self,
        request: Request<Streaming<HeartbeatRequest>>,
    ) -> StdResult<Response<Self::HeartbeatStream>, Status> {
        let volume_size_limit = self.volume_size_limit_mb;
        let topology = self.topology.clone();
        let addr = request.remote_addr().unwrap();

        let mut in_stream = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        tokio::spawn(async move {
            while let Some(result) = in_stream.next().await {
                match result {
                    Ok(heartbeat) => {
                        debug!("receive {:?}", heartbeat);
                        match handle_heartbeat(heartbeat, &topology, volume_size_limit, addr).await
                        {
                            Ok(resp) => {
                                let _ = tx.send(Ok(resp));
                            }
                            Err(err) => {
                                error!("handle heartbeat error: {err}");
                            }
                        }
                    }
                    Err(err) => {
                        if let Err(e) = tx.send(Err(err)) {
                            error!("heartbeat response dropped: {}", e);
                            return;
                        }
                    }
                }
            }
        });

        let out_stream = UnboundedReceiverStream::new(rx);
        Ok(Response::new(Box::pin(out_stream) as Self::HeartbeatStream))
    }

    async fn lookup_volume(
        &self,
        request: Request<LookupVolumeRequest>,
    ) -> StdResult<Response<LookupVolumeResponse>, Status> {
        let request = request.into_inner();
        if request.volumes.is_empty() {
            return Err(Status::invalid_argument("volumes can't be empty"));
        }

        let mut volume_locations = vec![];
        for mut volume_id in request.volumes {
            if let Some(idx) = volume_id.rfind(',') {
                let _fid = volume_id.split_off(idx);
            }
            let volume_id = volume_id.parse().map_err(|err: ParseIntError| {
                Status::invalid_argument(format!("parse volume id error: {err}"))
            })?;

            let mut locations = vec![];
            let mut error = String::default();
            match self
                .topology
                .write()
                .await
                .lookup(&request.collection, volume_id)
                .await
            {
                Some(nodes) => {
                    for dn in nodes.iter() {
                        let public_url = dn.read().await.public_url.to_string();
                        locations.push(Location {
                            url: dn.read().await.url(),
                            public_url,
                        });
                    }
                }
                None => {
                    error = format!("volume {volume_id} is not found");
                }
            }

            volume_locations.push(VolumeLocation {
                volume_id,
                locations,
                error,
            });
        }

        Ok(Response::new(LookupVolumeResponse { volume_locations }))
    }

    async fn lookup_ec_volume(
        &self,
        _request: Request<LookupEcVolumeRequest>,
    ) -> StdResult<Response<LookupEcVolumeResponse>, Status> {
        todo!()
    }
}

async fn handle_heartbeat(
    heartbeat: HeartbeatRequest,
    topology: &TopologyRef,
    volume_size_limit: u64,
    addr: SocketAddr,
) -> Result<HeartbeatResponse> {
    topology
        .write()
        .await
        .set_max_sequence(heartbeat.max_file_key);
    let mut ip = heartbeat.ip.clone();
    if heartbeat.ip.is_empty() {
        ip = addr.ip().to_string();
    }

    let data_center = get_or_default(heartbeat.data_center);
    let rack = get_or_default(heartbeat.rack);

    let mut topology = topology.write().await;
    let weak_topo = (*topology).downgrade();

    let data_center = topology.get_or_create_data_center(data_center).await;
    data_center.set_topology(weak_topo);
    let weak_dc = data_center.downgrade();

    let rack = data_center.get_or_create_rack(rack);
    rack.set_data_center(weak_dc);

    let node_addr = format!("{}:{}", ip, heartbeat.port);
    let node = rack
        .get_or_create_data_node(
            FastStr::new(node_addr),
            FastStr::new(ip),
            heartbeat.port as u16,
            FastStr::new(heartbeat.public_url),
            heartbeat.max_volume_count as u64,
        )
        .await?;
    node.write().await.set_rack(rack.downgrade());

    let mut infos = vec![];
    for info_msg in heartbeat.volumes {
        match VolumeInfo::new(info_msg) {
            Ok(info) => infos.push(info),
            Err(err) => info!("fail to convert joined volume: {err}"),
        };
    }

    let deleted_volumes = node.write().await.update_volumes(infos.clone()).await;

    for v in infos {
        topology.register_volume_layout(&v, node.clone()).await;
    }

    for v in deleted_volumes {
        topology.unregister_volume_layout(&v);
    }

    let leader = topology.current_leader_address().await.unwrap_or_default();

    Ok(HeartbeatResponse {
        volume_size_limit,
        leader: leader.to_string(),
        ..Default::default()
    })
}
