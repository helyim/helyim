use std::{pin::Pin, result::Result as StdResult, sync::Arc, time::Duration};

use async_stream::stream;
use axum::{routing::get, Router};
use faststr::FastStr;
use futures::{channel::mpsc::unbounded, StreamExt};
use ginepro::LoadBalancedChannel;
use helyim_proto::{
    helyim_client::HelyimClient,
    volume_server_server::{VolumeServer, VolumeServerServer},
    AllocateVolumeRequest, AllocateVolumeResponse, HeartbeatResponse, VacuumVolumeCheckRequest,
    VacuumVolumeCheckResponse, VacuumVolumeCleanupRequest, VacuumVolumeCleanupResponse,
    VacuumVolumeCommitRequest, VacuumVolumeCommitResponse, VacuumVolumeCompactRequest,
    VacuumVolumeCompactResponse, VolumeEcBlobDeleteRequest, VolumeEcBlobDeleteResponse,
    VolumeEcShardReadRequest, VolumeEcShardReadResponse, VolumeEcShardsCopyRequest,
    VolumeEcShardsCopyResponse, VolumeEcShardsDeleteRequest, VolumeEcShardsDeleteResponse,
    VolumeEcShardsGenerateRequest, VolumeEcShardsGenerateResponse, VolumeEcShardsMountRequest,
    VolumeEcShardsMountResponse, VolumeEcShardsRebuildRequest, VolumeEcShardsRebuildResponse,
    VolumeEcShardsToVolumeRequest, VolumeEcShardsToVolumeResponse, VolumeEcShardsUnmountRequest,
    VolumeEcShardsUnmountResponse,
};
use tokio::task::JoinHandle;
use tokio_stream::Stream;
use tonic::{transport::Server as TonicServer, Request, Response, Status, Streaming};
use tracing::{error, info};

use crate::{
    errors::Result,
    operation::Looker,
    rt_spawn,
    storage::{
        api::{fallback_handler, status_handler, StorageContext},
        needle_map::NeedleMapType,
        store::{store_loop, Store, StoreEventTx},
    },
    util::exit,
    STOP_INTERVAL,
};

pub struct StorageServer {
    host: FastStr,
    port: u16,
    pub master_node: FastStr,
    pub pulse_seconds: i64,
    pub data_center: FastStr,
    pub rack: FastStr,
    pub store: StoreEventTx,
    pub needle_map_type: NeedleMapType,
    pub read_redirect: bool,
    handles: Vec<JoinHandle<()>>,

    shutdown: async_broadcast::Sender<()>,
}

impl StorageServer {
    pub async fn new(
        host: &str,
        ip: &str,
        port: u16,
        public_url: &str,
        folders: Vec<String>,
        max_counts: Vec<i64>,
        needle_map_type: NeedleMapType,
        master_node: &str,
        pulse_seconds: i64,
        data_center: &str,
        rack: &str,
        read_redirect: bool,
    ) -> Result<StorageServer> {
        let (shutdown, mut shutdown_rx) = async_broadcast::broadcast(16);

        let master_node = FastStr::new(master_node);

        let store = Store::new(
            ip,
            port,
            public_url,
            folders,
            max_counts,
            needle_map_type,
            master_node.clone(),
            shutdown_rx.clone(),
        )
        .await?;

        let (tx, rx) = unbounded();
        let store_tx = StoreEventTx::new(tx);
        rt_spawn(store_loop(store, rx, shutdown_rx.clone()));

        let storage = StorageServer {
            host: FastStr::new(host),
            port,
            master_node,
            pulse_seconds,
            data_center: FastStr::new(data_center),
            rack: FastStr::new(rack),
            needle_map_type,
            read_redirect,
            store: store_tx.clone(),
            handles: vec![],
            shutdown,
        };

        let addr = format!("{}:{}", host, port + 1);
        let addr = addr.parse()?;

        rt_spawn(async move {
            info!("volume grpc server starting up. binding addr: {addr}");
            if let Err(err) = TonicServer::builder()
                .add_service(VolumeServerServer::new(StorageGrpcServer {
                    store: store_tx,
                    needle_map_type,
                }))
                .serve_with_shutdown(addr, async {
                    let _ = shutdown_rx.recv().await;
                })
                .await
            {
                error!("volume grpc server starting failed, {err}");
                exit();
            }
        });

        Ok(storage)
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

    fn grpc_addr(&self) -> Result<(String, u16)> {
        match self.master_node.rfind(':') {
            Some(idx) => {
                let port = self.master_node[idx + 1..].parse::<u16>()?;
                Ok((self.master_node[..idx].to_string(), port + 1))
            }
            None => Ok((self.master_node.to_string(), 80)),
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        let store = self.store.clone();
        let needle_map_type = self.needle_map_type;
        let read_redirect = self.read_redirect;
        let pulse_seconds = self.pulse_seconds as u64;

        let channel = LoadBalancedChannel::builder(self.grpc_addr()?)
            .channel()
            .await?;
        let client = HelyimClient::new(channel);

        let ctx = StorageContext {
            store,
            needle_map_type,
            read_redirect,
            pulse_seconds,
            client: client.clone(),
            looker: Arc::new(Looker::new()),
        };

        self.handles.push(
            start_heartbeat(
                self.store.clone(),
                client,
                self.pulse_seconds,
                self.shutdown.new_receiver(),
            )
            .await,
        );

        // http server
        let addr_str = format!("{}:{}", self.host, self.port);
        let addr = addr_str.parse()?;
        let mut shutdown_rx = self.shutdown.new_receiver();

        self.handles.push(rt_spawn(async move {
            let app = Router::new()
                .route("/status", get(status_handler))
                .fallback(fallback_handler)
                .with_state(ctx);

            match hyper::Server::try_bind(&addr) {
                Ok(builder) => {
                    let server = builder.serve(app.into_make_service());
                    let graceful = server.with_graceful_shutdown(async {
                        let _ = shutdown_rx.recv().await;
                    });
                    info!("volume api server starting up. binding addr: {addr}");
                    match graceful.await {
                        Ok(()) => info!("storage server shutting down gracefully."),
                        Err(e) => error!("storage server stop failed, {}", e),
                    }
                }
                Err(err) => {
                    error!("starting volume api server failed, error: {err}");
                    exit();
                }
            }
        }));

        Ok(())
    }
}

async fn start_heartbeat(
    store: StoreEventTx,
    mut client: HelyimClient<LoadBalancedChannel>,
    pulse_seconds: i64,
    mut shutdown: async_broadcast::Receiver<()>,
) -> JoinHandle<()> {
    rt_spawn(async move {
        'next_heartbeat: loop {
            tokio::select! {
                stream = heartbeat_stream(
                    store.clone(),
                    &mut client,
                    pulse_seconds,
                    shutdown.clone(),
                ) => {
                    match stream {
                        Ok(mut stream) => {
                            info!("heartbeat starting up success");
                            while let Some(response) = stream.next().await {
                                match response {
                                    Ok(response) => {
                                        if let Err(err) = store.set_volume_size_limit(response.volume_size_limit) {
                                            error!("set volume_size_limit failed, error: {err}");
                                        }
                                    }
                                    Err(err) => {
                                        error!("send heartbeat error: {err}, will try again after 4s.");
                                        tokio::time::sleep(STOP_INTERVAL * 2).await;
                                        continue 'next_heartbeat;
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            error!("heartbeat starting up failed: {err}, will try agent after 4s.");
                            tokio::time::sleep(STOP_INTERVAL * 2).await;
                        }
                    }
                }
                _ = shutdown.recv() => {
                    info!("stopping heartbeat.");
                    return;
                }
            }
        }
    })
}

async fn heartbeat_stream(
    store: StoreEventTx,
    client: &mut HelyimClient<LoadBalancedChannel>,
    pulse_seconds: i64,
    mut shutdown_rx: async_broadcast::Receiver<()>,
) -> Result<Streaming<HeartbeatResponse>> {
    let mut interval = tokio::time::interval(Duration::from_secs(pulse_seconds as u64));

    let request_stream = stream! {
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    match store.collect_heartbeat().await {
                        Ok(heartbeat) => yield heartbeat,
                        Err(err) => error!("collect heartbeat error: {err}")
                    }
                }
                // to avoid server side got `channel closed` error
                _ = shutdown_rx.recv() => {
                    break;
                }
            }
        }
    };
    let response = client.heartbeat(request_stream).await?;
    Ok(response.into_inner())
}

#[derive(Clone)]
struct StorageGrpcServer {
    store: StoreEventTx,
    needle_map_type: NeedleMapType,
}

#[tonic::async_trait]
impl VolumeServer for StorageGrpcServer {
    async fn allocate_volume(
        &self,
        request: Request<AllocateVolumeRequest>,
    ) -> StdResult<Response<AllocateVolumeResponse>, Status> {
        let request = request.into_inner();
        self.store
            .add_volume(
                request.volumes,
                request.collection,
                self.needle_map_type,
                request.replication,
                request.ttl,
                request.preallocate,
            )
            .await?;
        Ok(Response::new(AllocateVolumeResponse {}))
    }

    async fn vacuum_volume_check(
        &self,
        request: Request<VacuumVolumeCheckRequest>,
    ) -> StdResult<Response<VacuumVolumeCheckResponse>, Status> {
        let request = request.into_inner();
        info!("vacuum volume {} check", request.volume_id);
        let garbage_ratio = self.store.check_compact_volume(request.volume_id).await?;
        Ok(Response::new(VacuumVolumeCheckResponse { garbage_ratio }))
    }

    async fn vacuum_volume_compact(
        &self,
        request: Request<VacuumVolumeCompactRequest>,
    ) -> StdResult<Response<VacuumVolumeCompactResponse>, Status> {
        let request = request.into_inner();
        info!("vacuum volume {} compact", request.volume_id);
        self.store
            .compact_volume(request.volume_id, request.preallocate)
            .await?;
        Ok(Response::new(VacuumVolumeCompactResponse {}))
    }

    async fn vacuum_volume_commit(
        &self,
        request: Request<VacuumVolumeCommitRequest>,
    ) -> StdResult<Response<VacuumVolumeCommitResponse>, Status> {
        let request = request.into_inner();
        info!("vacuum volume {} commit compaction", request.volume_id);
        self.store.commit_compact_volume(request.volume_id).await?;
        // TODO: check whether the volume is read only
        Ok(Response::new(VacuumVolumeCommitResponse {
            is_read_only: false,
        }))
    }

    async fn vacuum_volume_cleanup(
        &self,
        request: Request<VacuumVolumeCleanupRequest>,
    ) -> StdResult<Response<VacuumVolumeCleanupResponse>, Status> {
        let request = request.into_inner();
        info!("vacuum volume {} cleanup", request.volume_id);
        self.store.commit_cleanup_volume(request.volume_id).await?;
        Ok(Response::new(VacuumVolumeCleanupResponse {}))
    }

    async fn volume_ec_shards_generate(
        &self,
        request: Request<VolumeEcShardsGenerateRequest>,
    ) -> StdResult<Response<VolumeEcShardsGenerateResponse>, Status> {
        todo!()
    }

    async fn volume_ec_shards_rebuild(
        &self,
        request: Request<VolumeEcShardsRebuildRequest>,
    ) -> StdResult<Response<VolumeEcShardsRebuildResponse>, Status> {
        todo!()
    }

    async fn volume_ec_shards_copy(
        &self,
        request: Request<VolumeEcShardsCopyRequest>,
    ) -> StdResult<Response<VolumeEcShardsCopyResponse>, Status> {
        todo!()
    }

    async fn volume_ec_shards_delete(
        &self,
        request: Request<VolumeEcShardsDeleteRequest>,
    ) -> StdResult<Response<VolumeEcShardsDeleteResponse>, Status> {
        todo!()
    }

    async fn volume_ec_shards_mount(
        &self,
        request: Request<VolumeEcShardsMountRequest>,
    ) -> StdResult<Response<VolumeEcShardsMountResponse>, Status> {
        todo!()
    }

    async fn volume_ec_shards_unmount(
        &self,
        request: Request<VolumeEcShardsUnmountRequest>,
    ) -> StdResult<Response<VolumeEcShardsUnmountResponse>, Status> {
        todo!()
    }

    type VolumeEcShardReadStream =
        Pin<Box<dyn Stream<Item = StdResult<VolumeEcShardReadResponse, Status>> + Send>>;

    async fn volume_ec_shard_read(
        &self,
        request: Request<VolumeEcShardReadRequest>,
    ) -> StdResult<Response<Self::VolumeEcShardReadStream>, Status> {
        todo!()
    }

    async fn volume_ec_blob_delete(
        &self,
        request: Request<VolumeEcBlobDeleteRequest>,
    ) -> StdResult<Response<VolumeEcBlobDeleteResponse>, Status> {
        todo!()
    }

    /// generate the .idx, .dat, files from .ecx, .ecj and .ec01 - .ec14 files
    async fn volume_ec_shards_to_volume(
        &self,
        request: Request<VolumeEcShardsToVolumeRequest>,
    ) -> StdResult<Response<VolumeEcShardsToVolumeResponse>, Status> {
        todo!()
    }
}
