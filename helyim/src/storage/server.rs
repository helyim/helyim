use std::{
    ffi::OsString, fs, net::SocketAddr, os::unix::fs::FileExt, path::Path, pin::Pin,
    result::Result as StdResult, sync::Arc, time::Duration,
};

use async_stream::stream;
use axum::{extract::DefaultBodyLimit, routing::get, Router};
use faststr::FastStr;
use futures::StreamExt;
use helyim_proto::{
    volume_server_server::{VolumeServer as HelyimVolumeServer, VolumeServerServer},
    AllocateVolumeRequest, AllocateVolumeResponse, VacuumVolumeCheckRequest,
    VacuumVolumeCheckResponse, VacuumVolumeCleanupRequest, VacuumVolumeCleanupResponse,
    VacuumVolumeCommitRequest, VacuumVolumeCommitResponse, VacuumVolumeCompactRequest,
    VacuumVolumeCompactResponse, VolumeEcBlobDeleteRequest, VolumeEcBlobDeleteResponse,
    VolumeEcShardReadRequest, VolumeEcShardReadResponse, VolumeEcShardsCopyRequest,
    VolumeEcShardsCopyResponse, VolumeEcShardsDeleteRequest, VolumeEcShardsDeleteResponse,
    VolumeEcShardsGenerateRequest, VolumeEcShardsGenerateResponse, VolumeEcShardsMountRequest,
    VolumeEcShardsMountResponse, VolumeEcShardsRebuildRequest, VolumeEcShardsRebuildResponse,
    VolumeEcShardsToVolumeRequest, VolumeEcShardsToVolumeResponse, VolumeEcShardsUnmountRequest,
    VolumeEcShardsUnmountResponse, VolumeInfo,
};
use tokio::time::sleep;
use tokio_stream::{wrappers::UnboundedReceiverStream, Stream};
use tonic::{transport::Server as TonicServer, Request, Response, Status};
use tower_http::{compression::CompressionLayer, timeout::TimeoutLayer};
use tracing::{debug, error, info, warn};

use crate::{
    errors::Result,
    operation::{list_master, Looker},
    proto::save_volume_info,
    rt_spawn,
    storage::{
        api::{delete_handler, get_or_head_handler, post_handler, status_handler, StorageState},
        erasure_coding::{
            ec_shard_base_filename, find_data_filesize, rebuild_ec_files, rebuild_ecx_file, to_ext,
            write_data_file, write_ec_files, write_index_file_from_ec_index,
            write_sorted_file_from_index, ShardId,
        },
        needle::NeedleMapType,
        store::{Store, StoreRef},
        version::Version,
        VolumeError, BUFFER_SIZE_LIMIT,
    },
    util::{
        args::VolumeOptions,
        file::file_exists,
        grpc::{grpc_port, helyim_client},
        http::{default_handler, favicon_handler},
        sys::exit,
    },
};

pub struct VolumeServer {
    pub options: Arc<VolumeOptions>,
    pub store: StoreRef,
    pub needle_map_type: NeedleMapType,
    pub read_redirect: bool,
    pub current_master: FastStr,
    pub seed_master_nodes: Vec<FastStr>,

    shutdown: async_broadcast::Sender<()>,
}

impl VolumeServer {
    pub async fn new(
        needle_map_type: NeedleMapType,
        volume_opts: VolumeOptions,
        read_redirect: bool,
    ) -> Result<VolumeServer> {
        let (shutdown, mut shutdown_rx) = async_broadcast::broadcast(16);

        let options = Arc::new(volume_opts);
        let store = Arc::new(Store::new(options.clone(), needle_map_type).await?);

        let addr = format!("{}:{}", options.ip, grpc_port(options.port)).parse()?;

        let storage = VolumeServer {
            options,
            needle_map_type,
            read_redirect,
            current_master: FastStr::empty(),
            seed_master_nodes: Vec::new(),
            store: store.clone(),
            shutdown,
        };

        rt_spawn(async move {
            info!("volume grpc server starting up. binding addr: {addr}");
            if let Err(err) = TonicServer::builder()
                .add_service(VolumeServerServer::new(StorageGrpcServer {
                    store,
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

    pub async fn stop(self) -> Result<()> {
        self.shutdown.broadcast(()).await?;
        Ok(())
    }

    pub async fn start(&mut self) -> Result<()> {
        let store = self.store.clone();
        let needle_map_type = self.needle_map_type;
        let read_redirect = self.read_redirect;
        let pulse = self.options.pulse;

        self.update_masters().await?;

        let ctx = StorageState {
            store,
            needle_map_type,
            read_redirect,
            pulse,
            looker: Arc::new(Looker::new()),
        };

        rt_spawn(Self::heartbeat(
            self.store.clone(),
            self.seed_master_nodes.clone(),
            pulse,
            self.shutdown.new_receiver(),
        ));

        // http server
        let addr = format!("{}:{}", self.options.ip, self.options.port).parse()?;
        let shutdown_rx = self.shutdown.new_receiver();

        rt_spawn(start_volume_server(ctx, addr, shutdown_rx));

        Ok(())
    }
}

impl VolumeServer {
    async fn update_masters(&mut self) -> StdResult<(), VolumeError> {
        let cluster_status = list_master(&self.options.master_server).await?;
        self.current_master = FastStr::new(cluster_status.leader);
        self.seed_master_nodes = cluster_status.peers;

        Ok(())
    }
}

impl VolumeServer {
    async fn heartbeat(
        store: StoreRef,
        seed_masters: Vec<FastStr>,
        pulse: u64,
        mut shutdown: async_broadcast::Receiver<()>,
    ) {
        let mut new_leader = FastStr::empty();
        loop {
            for master in seed_masters.iter() {
                if !new_leader.is_empty() && &new_leader != master {
                    sleep(Duration::from_secs(pulse)).await;
                    continue;
                }
                store.set_current_master(master.clone()).await;
                tokio::select! {
                    ret = VolumeServer::do_heartbeat(
                        master,
                        store.clone(),
                        pulse,
                        shutdown.clone(),
                    ) => {
                            match ret {
                                Err(VolumeError::LeaderChanged(new, old)) => {
                                    if !new.is_empty() {
                                        new_leader = new;
                                    }
                                }
                                Err(err @ (VolumeError::StartHeartbeat | VolumeError::SendHeartbeat(_))) => {
                                    warn!("heartbeat to {master} error: {err}");
                                    sleep(Duration::from_secs(pulse)).await;
                                    new_leader = FastStr::empty();
                                    store.set_current_master(FastStr::empty()).await;
                                }
                                Err(err) => {
                                    error!("heartbeat but error occur: {err}");
                                }
                                _ => {}
                        }
                    }
                    _ = shutdown.recv() => {
                        info!("stopping heartbeat.");
                        return;
                    }
                }
            }
        }
    }

    async fn do_heartbeat(
        master: &FastStr,
        store: StoreRef,
        pulse: u64,
        mut shutdown_rx: async_broadcast::Receiver<()>,
    ) -> StdResult<(), VolumeError> {
        let mut interval = tokio::time::interval(Duration::from_secs(pulse));

        let store_ref = store.clone();
        let request_stream = stream! {
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        match store_ref.collect_heartbeat().await {
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

        let client = helyim_client(master)?;
        match client.heartbeat(request_stream).await {
            Ok(response) => {
                let mut stream = response.into_inner();
                info!("heartbeat client starting up success, will heartbeat to {master}");
                while let Some(response) = stream.next().await {
                    match response {
                        Ok(response) => {
                            if let Ok(response) = serde_json::to_string(&response) {
                                debug!("heartbeat reply: {response}");
                            }
                            let old_leader = store.current_master.read().await.clone();
                            if !response.leader.is_empty() && response.leader != old_leader {
                                info!(
                                    "leader changed, current leader is {}, old leader is \
                                     {old_leader}",
                                    response.leader
                                );
                                let new_leader = FastStr::new(response.leader);
                                store.set_current_master(new_leader.clone()).await;
                                return Err(VolumeError::LeaderChanged(new_leader, old_leader));
                            }
                            store.set_volume_size_limit(response.volume_size_limit);
                        }
                        Err(err) => {
                            error!(
                                "send heartbeat to {master} error: {err}, will try again after \
                                 {pulse}s."
                            );
                            return Err(VolumeError::SendHeartbeat(master.clone()));
                        }
                    }
                }
                Ok(())
            }
            Err(err) => {
                error!("heartbeat starting up failed: {err}, will try agent after {pulse}s.");
                Err(VolumeError::StartHeartbeat)
            }
        }
    }
}

async fn start_volume_server(
    ctx: StorageState,
    addr: SocketAddr,
    mut shutdown: async_broadcast::Receiver<()>,
) {
    let app = Router::new()
        .route("/", get(default_handler))
        .route("/status", get(status_handler))
        .route("/favicon.ico", get(favicon_handler))
        .fallback_service(
            get(get_or_head_handler)
                .head(get_or_head_handler)
                .post(post_handler)
                .delete(delete_handler)
                .fallback(default_handler)
                .with_state(ctx.clone()),
        )
        .layer((
            CompressionLayer::new(),
            DefaultBodyLimit::max(1024 * 1024 * 50),
            TimeoutLayer::new(Duration::from_secs(10)),
        ))
        .with_state(ctx);

    match hyper::Server::try_bind(&addr) {
        Ok(builder) => {
            let server = builder.serve(app.into_make_service());
            let graceful = server.with_graceful_shutdown(async {
                let _ = shutdown.recv().await;
            });
            info!("volume api server starting up. binding addr: {addr}");
            match graceful.await {
                Ok(()) => info!("volume api server shutting down gracefully."),
                Err(e) => error!("volume api server stop failed, {}", e),
            }
        }
        Err(err) => {
            error!("starting volume api server failed, error: {err}");
            exit();
        }
    }
}

#[derive(Clone)]
struct StorageGrpcServer {
    store: StoreRef,
    needle_map_type: NeedleMapType,
}

#[tonic::async_trait]
impl HelyimVolumeServer for StorageGrpcServer {
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
        debug!("vacuum volume {} check", request.volume_id);
        let garbage_ratio = self.store.check_compact_volume(request.volume_id).await?;
        Ok(Response::new(VacuumVolumeCheckResponse { garbage_ratio }))
    }

    async fn vacuum_volume_compact(
        &self,
        request: Request<VacuumVolumeCompactRequest>,
    ) -> StdResult<Response<VacuumVolumeCompactResponse>, Status> {
        let request = request.into_inner();
        debug!("vacuum volume {} compact", request.volume_id);
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
        debug!("vacuum volume {} commit compaction", request.volume_id);
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
        debug!("vacuum volume {} cleanup", request.volume_id);
        self.store.commit_cleanup_volume(request.volume_id).await?;
        Ok(Response::new(VacuumVolumeCleanupResponse {}))
    }

    async fn volume_ec_shards_generate(
        &self,
        request: Request<VolumeEcShardsGenerateRequest>,
    ) -> StdResult<Response<VolumeEcShardsGenerateResponse>, Status> {
        let request = request.into_inner();
        match self.store.find_volume(request.volume_id).await? {
            Some(volume) => {
                let base_filename = volume.filename();
                let collection = volume.collection.clone();
                if collection != request.collection {
                    return Err(Status::invalid_argument(format!(
                        "invalid collection, expect: {collection}"
                    )));
                }
                write_ec_files(&base_filename)?;
                write_sorted_file_from_index(&base_filename, ".ecx")?;
                let volume_info = VolumeInfo {
                    version: volume.version() as u32,
                    ..Default::default()
                };
                save_volume_info(&format!("{}.vif", base_filename), volume_info)?;
                Ok(Response::new(VolumeEcShardsGenerateResponse::default()))
            }
            None => Err(Status::not_found(format!(
                "volume {} is not found.",
                request.volume_id
            ))),
        }
    }

    async fn volume_ec_shards_rebuild(
        &self,
        request: Request<VolumeEcShardsRebuildRequest>,
    ) -> StdResult<Response<VolumeEcShardsRebuildResponse>, Status> {
        let request = request.into_inner();
        let base_filename = ec_shard_base_filename(&request.collection, request.volume_id);

        let mut rebuilt_shard_ids = Vec::new();
        for location in self.store.locations().iter() {
            let ecx_filename = format!("{}{}.ecx", location.directory, base_filename);
            if file_exists(&ecx_filename)? {
                let base_filename = format!("{}{}", location.directory, base_filename);
                rebuilt_shard_ids.extend(rebuild_ec_files(&base_filename)?);
                rebuild_ecx_file(&base_filename)?;
                break;
            }
        }

        Ok(Response::new(VolumeEcShardsRebuildResponse {
            rebuilt_shard_ids,
        }))
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
        let request = request.into_inner();
        let mut base_filename = ec_shard_base_filename(&request.collection, request.volume_id);
        let mut found = false;

        for location in self.store.locations().iter() {
            let ecx_filename = format!("{}{}.ecx", location.directory, base_filename);
            if file_exists(&ecx_filename)? {
                found = true;
                base_filename = format!("{}{}", location.directory, base_filename);
                for shard in request.shard_ids {
                    fs::remove_file(format!("{}{}", base_filename, to_ext(shard as ShardId)))?;
                }
                break;
            }
        }

        if !found {
            return Ok(Response::new(VolumeEcShardsDeleteResponse {}));
        }

        let mut has_ecx_file = false;
        let mut has_idx_file = false;
        let mut existing_shard_count = 0;

        let filename = Path::new(&base_filename)
            .file_name()
            .map(|name| name.to_os_string())
            .unwrap_or(OsString::from("."));
        let filename = filename.to_string_lossy().to_string();

        let ecx_filename = format!("{}.ecx", filename);
        let ecj_filename = format!("{}.ecj", filename);
        let idx_filename = format!("{}.idx", filename);
        let ec_prefix = format!("{}.ec", filename);

        for location in self.store.locations().iter() {
            let read_dir = fs::read_dir(location.directory.to_string())?;
            for entry in read_dir {
                match entry?.file_name().into_string() {
                    Ok(entry_name) => {
                        if entry_name == ecx_filename || entry_name == ecj_filename {
                            has_ecx_file = true;
                            continue;
                        }
                        if entry_name == idx_filename {
                            has_idx_file = true;
                            continue;
                        }
                        if entry_name.starts_with(&ec_prefix) {
                            existing_shard_count += 1;
                        }
                    }
                    Err(err) => {
                        return Err(Status::internal(err.to_string_lossy().to_string()));
                    }
                }
            }
        }

        if has_ecx_file && existing_shard_count == 0 {
            fs::remove_file(format!("{}.ecx", base_filename))?;
            fs::remove_file(format!("{}.ecj", base_filename))?;
        }

        if !has_idx_file {
            fs::remove_file(format!("{}.vif", base_filename))?;
        }

        Ok(Response::new(VolumeEcShardsDeleteResponse {}))
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
        let request = request.into_inner();

        if let Some(volume) = self.store.find_ec_volume(request.volume_id).await {
            if let Some(shard) = volume.read().await.find_shard(request.shard_id as ShardId) {
                if request.file_key != 0 {
                    let needle_value =
                        volume.read().await.find_needle_from_ecx(request.file_key)?;
                    if needle_value.size.is_deleted() {
                        let response = VolumeEcShardReadResponse {
                            is_deleted: true,
                            ..Default::default()
                        };
                        let stream = Box::pin(futures::stream::iter(vec![Ok(response)]));
                        return Ok(Response::new(stream as Self::VolumeEcShardReadStream));
                    }
                }

                let mut buf_size = request.size as usize;
                if buf_size > BUFFER_SIZE_LIMIT {
                    buf_size = BUFFER_SIZE_LIMIT;
                }

                let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

                tokio::spawn(async move {
                    let mut buffer = vec![0u8; buf_size];
                    let mut start_offset = request.offset as u64;
                    let mut bytes_to_read = request.size as usize;
                    while bytes_to_read > 0 {
                        let mut buffer_size = buf_size;
                        if buffer_size > bytes_to_read {
                            buffer_size = bytes_to_read;
                        }
                        let mut bytes_read = shard
                            .ecd_file
                            .read_at(&mut buffer[0..buffer_size], start_offset)
                            .unwrap_or_default();
                        if bytes_read > 0 {
                            if bytes_read > bytes_to_read {
                                bytes_read = bytes_to_read;
                            }
                            if let Err(err) = tx.send(Ok(VolumeEcShardReadResponse {
                                is_deleted: false,
                                data: buffer[..bytes_read].to_vec(),
                            })) {
                                error!("send VolumeEcShardReadResponse error: {err}");
                                break;
                            }

                            start_offset += bytes_read as u64;
                            bytes_to_read -= bytes_read;
                        }
                    }
                });

                let stream = UnboundedReceiverStream::new(rx);
                return Ok(Response::new(
                    Box::pin(stream) as Self::VolumeEcShardReadStream
                ));
            }
        }

        Err(Status::not_found(format!(
            "ec volume {} is not found",
            request.volume_id
        )))
    }

    async fn volume_ec_blob_delete(
        &self,
        request: Request<VolumeEcBlobDeleteRequest>,
    ) -> StdResult<Response<VolumeEcBlobDeleteResponse>, Status> {
        let request = request.into_inner();

        for location in self.store.locations().iter() {
            if let Some(volume) = location.find_ec_volume(request.volume_id) {
                let (needle_value, intervals) = volume
                    .read()
                    .await
                    .locate_ec_shard_needle(request.file_key, request.version as Version)?;
                if needle_value.size.is_deleted() {
                    return Ok(Response::new(VolumeEcBlobDeleteResponse::default()));
                }

                volume
                    .write()
                    .await
                    .delete_needle_from_ecx(request.file_key)?;
                break;
            }
        }
        Ok(Response::new(VolumeEcBlobDeleteResponse::default()))
    }

    /// generate the .idx, .dat, files from .ecx, .ecj and .ec01 - .ec14 files
    async fn volume_ec_shards_to_volume(
        &self,
        request: Request<VolumeEcShardsToVolumeRequest>,
    ) -> StdResult<Response<VolumeEcShardsToVolumeResponse>, Status> {
        let request = request.into_inner();

        match self.store.find_ec_volume(request.volume_id).await {
            Some(volume) => {
                if volume.read().await.collection() == request.collection {
                    return Err(Status::invalid_argument("unexpected collection"));
                }
                let base_filename = volume.read().await.filename();
                let data_filesize = find_data_filesize(&base_filename)?;
                write_data_file(&base_filename, data_filesize)?;
                write_index_file_from_ec_index(&base_filename)?;

                Ok(Response::new(VolumeEcShardsToVolumeResponse::default()))
            }
            None => Err(Status::not_found(format!(
                "ec volume {} not found",
                request.volume_id
            ))),
        }
    }
}
