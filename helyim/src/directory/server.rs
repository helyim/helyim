use std::{pin::Pin, result::Result as StdResult, sync::Arc};

use axum::{routing::get, Router};
use futures::{lock::Mutex, Stream, StreamExt};
use helyim_proto::{
    helyim_server::{Helyim, HelyimServer},
    Heartbeat, HeartbeatResponse,
};
use tokio::{sync::broadcast, task::JoinHandle};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{transport::Server as TonicServer, Request, Response, Status, Streaming};
use tracing::{error, info};

use crate::{
    default_handler,
    directory::{
        api::{
            assign_handler, cluster_status_handler, dir_status_handler, lookup_handler,
            DirectoryContext,
        },
        Topology, VolumeGrowth,
    },
    errors::Result,
    rt_spawn,
    sequence::{MemorySequencer, Sequencer},
    storage::{ReplicaPlacement, VolumeInfo},
    util::get_or_default,
    STOP_INTERVAL,
};

pub struct DirectoryServer {
    pub host: String,
    pub ip: String,
    pub port: u16,
    pub meta_folder: String,
    pub default_replica_placement: ReplicaPlacement,
    pub volume_size_limit_mb: u64,
    // pub preallocate: i64,
    // pub pulse_seconds: i64,
    pub garbage_threshold: f64,
    pub topology: Arc<Mutex<Topology>>,
    pub volume_grow: Arc<Mutex<VolumeGrowth>>,
    handles: Vec<JoinHandle<()>>,

    shutdown: broadcast::Sender<()>,
}

impl DirectoryServer {
    // TODO: add whiteList sk
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        host: &str,
        ip: &str,
        port: u16,
        meta_folder: &str,
        volume_size_limit_mb: u64,
        pulse_seconds: u64,
        default_replica_placement: ReplicaPlacement,
        garbage_threshold: f64,
        seq: MemorySequencer,
    ) -> Result<DirectoryServer> {
        let topology = Arc::new(Mutex::new(Topology::new(
            seq,
            volume_size_limit_mb * 1024 * 1024,
            pulse_seconds,
        )));

        let (shutdown, mut shutdown_rx) = broadcast::channel(1);

        let dir = DirectoryServer {
            host: host.to_string(),
            ip: ip.to_string(),
            volume_size_limit_mb,
            port,
            garbage_threshold,
            default_replica_placement,
            meta_folder: meta_folder.to_string(),
            volume_grow: Arc::new(Mutex::new(VolumeGrowth::new())),
            topology: topology.clone(),
            shutdown,
            handles: vec![],
        };

        let addr = format!("{}:{}", host, port + 1);
        let addr = addr.parse()?;

        rt_spawn(async move {
            if let Err(err) = TonicServer::builder()
                .add_service(HelyimServer::new(GrpcServer {
                    volume_size_limit_mb,
                    topology: topology.clone(),
                }))
                .serve_with_shutdown(addr, async {
                    let _ = shutdown_rx.recv().await;
                })
                .await
            {
                error!("grpc server starting failed, {}", err);
            }
        });

        Ok(dir)
    }

    pub async fn stop(&mut self) -> Result<()> {
        self.shutdown.send(())?;

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
        let mut shutdown_rx = self.shutdown.subscribe();

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
struct GrpcServer {
    pub volume_size_limit_mb: u64,
    pub topology: Arc<Mutex<Topology>>,
}

#[tonic::async_trait]
impl Helyim for GrpcServer {
    type SendHeartbeatStream =
        Pin<Box<dyn Stream<Item = StdResult<HeartbeatResponse, Status>> + Send>>;

    async fn send_heartbeat(
        &self,
        request: Request<Streaming<Heartbeat>>,
    ) -> StdResult<Response<Self::SendHeartbeatStream>, Status> {
        let volume_size_limit = self.volume_size_limit_mb;
        let topology = self.topology.clone();
        let addr = request.remote_addr().unwrap();

        let mut in_stream = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        tokio::spawn(async move {
            while let Some(result) = in_stream.next().await {
                match result {
                    Ok(heartbeat) => {
                        info!("received {:?}", heartbeat);

                        topology
                            .lock()
                            .await
                            .sequence
                            .set_max(heartbeat.max_file_key);
                        let mut ip = heartbeat.ip.clone();
                        if heartbeat.ip.is_empty() {
                            ip = addr.ip().to_string();
                        }

                        let data_center = get_or_default(heartbeat.data_center);
                        let rack = get_or_default(heartbeat.rack);

                        let data_center = topology
                            .lock()
                            .await
                            .get_or_create_data_center(&data_center);
                        let rack = data_center.lock().await.get_or_create_rack(&rack);
                        rack.lock().await.data_center = Arc::downgrade(&data_center);

                        let node_addr = format!("{}:{}", ip, heartbeat.port);
                        let node = rack.lock().await.get_or_create_data_node(
                            &node_addr,
                            &ip,
                            heartbeat.port as i64,
                            &heartbeat.public_url,
                            heartbeat.max_volume_count as i64,
                        );
                        node.lock().await.rack = Arc::downgrade(&rack);

                        let mut infos = vec![];
                        for info_msg in heartbeat.volumes.iter() {
                            match VolumeInfo::new(info_msg) {
                                Ok(info) => infos.push(info),
                                Err(err) => info!("fail to convert joined volume: {}", err),
                            };
                        }

                        let deleted_volumes = node.lock().await.update_volumes(infos.clone()).await;

                        for v in infos {
                            topology
                                .lock()
                                .await
                                .register_volume_layout(v, node.clone())
                                .await;
                        }

                        for v in deleted_volumes.iter() {
                            topology
                                .lock()
                                .await
                                .unregister_volume_layout(v.clone())
                                .await;
                        }

                        let resp = HeartbeatResponse {
                            volume_size_limit,
                            ..Default::default()
                        };

                        let _ = tx.send(Ok(resp));
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
        Ok(Response::new(
            Box::pin(out_stream) as Self::SendHeartbeatStream
        ))
    }
}
