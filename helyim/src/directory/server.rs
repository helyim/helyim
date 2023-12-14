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
use tokio::task::JoinHandle;
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
    raft::start_raft_node_with_peers,
    rt_spawn,
    sequence::Sequencer,
    storage::VolumeInfo,
    topology::{topology_vacuum_loop, volume_grow::VolumeGrowth, TopologyRef},
    util::{exit, get_or_default, http::default_handler},
    STOP_INTERVAL,
};

pub struct DirectoryServer {
    pub host: FastStr,
    pub ip: FastStr,
    pub port: u16,
    pub meta_folder: FastStr,
    pub default_replication: FastStr,
    pub volume_size_limit_mb: u64,
    pub pulse_seconds: u64,
    pub garbage_threshold: f64,
    pub topology: TopologyRef,
    pub volume_grow: Arc<VolumeGrowth>,
    handles: Vec<JoinHandle<()>>,

    shutdown: async_broadcast::Sender<()>,
}

impl DirectoryServer {
    pub async fn new(
        host: &str,
        ip: &str,
        port: u16,
        meta_folder: &str,
        volume_size_limit_mb: u64,
        pulse_seconds: u64,
        default_replication: &str,
        peers: &[String],
        garbage_threshold: f64,
        sequencer: Sequencer,
    ) -> Result<DirectoryServer> {
        let (shutdown, mut shutdown_rx) = async_broadcast::broadcast(16);

        let topology =
            TopologyRef::new(sequencer, volume_size_limit_mb * 1024 * 1024, pulse_seconds);

        // start raft node and control cluster with `raft_client`
        let raft_server = start_raft_node_with_peers(peers, shutdown_rx.clone()).await?;
        raft_server.set_topology(&topology).await;
        topology.write().await.set_raft_server(raft_server);

        let topology_vacuum_handle = rt_spawn(topology_vacuum_loop(
            topology.clone(),
            garbage_threshold,
            volume_size_limit_mb * (1 << 20),
            shutdown_rx.clone(),
        ));

        let dir = DirectoryServer {
            host: FastStr::new(host),
            ip: FastStr::new(ip),
            volume_size_limit_mb,
            port,
            garbage_threshold,
            pulse_seconds,
            default_replication: FastStr::new(default_replication),
            meta_folder: FastStr::new(meta_folder),
            volume_grow: Arc::new(VolumeGrowth),
            topology: topology.clone(),
            shutdown,
            handles: vec![topology_vacuum_handle],
        };

        let addr = format!("{}:{}", host, port + 1).parse()?;
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

        Ok(dir)
    }

    pub async fn stop(&mut self) -> Result<()> {
        self.shutdown.broadcast(()).await?;

        let mut interval = tokio::time::interval(STOP_INTERVAL);

        loop {
            self.handles.retain(|handle| !handle.is_finished());
            if self.handles.is_empty() {
                break;
            }
            interval.tick().await;
        }
        Ok(())
    }

    pub async fn start(&mut self) -> Result<()> {
        let ctx = DirectoryContext {
            topology: self.topology.clone(),
            volume_grow: self.volume_grow.clone(),
            default_replication: self.default_replication.clone(),
            ip: self.ip.clone(),
            port: self.port,
        };

        // http server
        let addr = format!("{}:{}", self.host, self.port).parse()?;
        let mut shutdown_rx = self.shutdown.new_receiver();

        let handle = rt_spawn(async move {
            let app = Router::new()
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

            match hyper::Server::try_bind(&addr) {
                Ok(builder) => {
                    let server = builder.serve(app.into_make_service());
                    let graceful = server.with_graceful_shutdown(async {
                        let _ = shutdown_rx.recv().await;
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
        });
        self.handles.push(handle);

        Ok(())
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

    let data_center = topology
        .write()
        .await
        .get_or_create_data_center(data_center)
        .await;
    data_center.write().await.set_topology(topology.downgrade());

    let rack = data_center.write().await.get_or_create_rack(rack);
    rack.write().await.set_data_center(data_center.downgrade());

    let node_addr = format!("{}:{}", ip, heartbeat.port);
    let node = rack
        .write()
        .await
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
        topology
            .write()
            .await
            .register_volume_layout(v, node.clone())
            .await;
    }

    for v in deleted_volumes.iter() {
        topology
            .write()
            .await
            .unregister_volume_layout(v.clone())
            .await;
    }

    Ok(HeartbeatResponse {
        volume_size_limit,
        ..Default::default()
    })
}
