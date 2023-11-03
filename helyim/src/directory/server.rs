use std::{net::SocketAddr, num::ParseIntError, pin::Pin, result::Result as StdResult, sync::Arc};

use axum::{response::Html, routing::get, Router};
use faststr::FastStr;
use futures::{channel::mpsc::unbounded, Stream, StreamExt};
use helyim_proto::{
    helyim_server::{Helyim, HelyimServer},
    lookup_volume_response::VolumeLocation,
    HeartbeatRequest, HeartbeatResponse, Location, LookupEcVolumeRequest, LookupEcVolumeResponse,
    LookupVolumeRequest, LookupVolumeResponse,
};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{transport::Server as TonicServer, Request, Response, Status, Streaming};
use tracing::{debug, error, info};

use crate::{
    directory::api::{
        assign_handler, cluster_status_handler, dir_status_handler, DirectoryContext,
    },
    errors::Result,
    rt_spawn,
    sequence::Sequencer,
    storage::{ReplicaPlacement, VolumeInfo},
    topology::{
        topology_loop, topology_vacuum_loop, volume_grow::VolumeGrowth, Topology, TopologyEventTx,
    },
    util::{exit, get_or_default},
    PHRASE, STOP_INTERVAL,
};

pub struct DirectoryServer {
    pub host: FastStr,
    pub ip: FastStr,
    pub port: u16,
    pub meta_folder: FastStr,
    pub default_replica_placement: ReplicaPlacement,
    pub volume_size_limit_mb: u64,
    pub pulse_seconds: u64,
    pub garbage_threshold: f64,
    pub topology: TopologyEventTx,
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
        default_replica_placement: ReplicaPlacement,
        garbage_threshold: f64,
        sequencer: Sequencer,
    ) -> Result<DirectoryServer> {
        let (shutdown, mut shutdown_rx) = async_broadcast::broadcast(16);

        // topology event loop
        let topology = Topology::new(
            sequencer,
            volume_size_limit_mb * 1024 * 1024,
            pulse_seconds,
            shutdown_rx.clone(),
        );
        let (tx, rx) = unbounded();
        let topology_handle = rt_spawn(topology_loop(topology, rx, shutdown_rx.clone()));
        let topology = TopologyEventTx::new(tx);
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
            default_replica_placement,
            meta_folder: FastStr::new(meta_folder),
            volume_grow: Arc::new(VolumeGrowth),
            topology: topology.clone(),
            shutdown,
            handles: vec![topology_handle, topology_vacuum_handle],
        };

        let addr = format!("{}:{}", host, port + 1).parse()?;

        rt_spawn(async move {
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
                error!("grpc server starting failed, {err}");
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
            default_replica_placement: self.default_replica_placement,
            ip: self.ip.clone(),
            port: self.port,
        };

        // http server
        let addr = format!("{}:{}", self.host, self.port);
        let addr = addr.parse()?;
        let mut shutdown_rx = self.shutdown.new_receiver();

        let handle = rt_spawn(async move {
            async fn default_handler() -> Html<&'static str> {
                Html(PHRASE)
            }

            let app = Router::new()
                .route("/dir/assign", get(assign_handler).post(assign_handler))
                .route(
                    "/dir/status",
                    get(dir_status_handler).post(dir_status_handler),
                )
                .route(
                    "/cluster/status",
                    get(cluster_status_handler).post(cluster_status_handler),
                )
                .fallback(default_handler)
                .with_state(ctx);

            let server = hyper::Server::bind(&addr).serve(app.into_make_service());
            let graceful = server.with_graceful_shutdown(async {
                let _ = shutdown_rx.recv().await;
            });
            info!("directory server starting up.");
            match graceful.await {
                Ok(()) => info!("directory server shutting down gracefully."),
                Err(e) => error!("directory server stop failed, {}", e),
            }
        });
        self.handles.push(handle);

        Ok(())
    }
}

#[derive(Clone)]
struct DirectoryGrpcServer {
    pub volume_size_limit_mb: u64,
    pub topology: TopologyEventTx,
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
        let collection = FastStr::from(request.collection);

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
            match self.topology.lookup(collection.clone(), volume_id).await {
                Ok(Some(nodes)) => {
                    for dn in nodes.iter() {
                        let public_url = dn.public_url.to_string();
                        locations.push(Location {
                            url: dn.url(),
                            public_url,
                        });
                    }
                }
                Ok(None) => {}
                Err(err) => {
                    error!("lookup volumes error: {err}");
                    error = err.to_string();
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
    topology: &TopologyEventTx,
    volume_size_limit: u64,
    addr: SocketAddr,
) -> Result<HeartbeatResponse> {
    topology.set_max_sequence(heartbeat.max_file_key)?;
    let mut ip = heartbeat.ip.clone();
    if heartbeat.ip.is_empty() {
        ip = addr.ip().to_string();
    }

    let data_center = get_or_default(heartbeat.data_center);
    let rack = get_or_default(heartbeat.rack);

    let data_center = topology.get_or_create_data_center(data_center).await?;
    let rack = data_center.get_or_create_rack(rack).await?;
    rack.set_data_center(data_center)?;

    let node_addr = format!("{}:{}", ip, heartbeat.port);
    let node = rack
        .get_or_create_data_node(
            FastStr::new(node_addr),
            FastStr::new(ip),
            heartbeat.port as u16,
            FastStr::new(heartbeat.public_url),
            heartbeat.max_volume_count as i64,
        )
        .await?;
    node.set_rack(Arc::downgrade(&rack))?;

    let mut infos = vec![];
    for info_msg in heartbeat.volumes {
        match VolumeInfo::new(info_msg) {
            Ok(info) => infos.push(info),
            Err(err) => info!("fail to convert joined volume: {err}"),
        };
    }

    let deleted_volumes = node.update_volumes(infos.clone()).await?;

    for v in infos {
        topology.register_volume_layout(v, node.clone()).await?;
    }

    for v in deleted_volumes.iter() {
        topology.unregister_volume_layout(v.clone())?;
    }

    Ok(HeartbeatResponse {
        volume_size_limit,
        ..Default::default()
    })
}
