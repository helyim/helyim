use std::{
    ffi::OsString, fs, os::unix::fs::FileExt, path::Path, pin::Pin, result::Result as StdResult,
    sync::Arc, time::Duration,
};

use async_stream::stream;
use axum::{extract::DefaultBodyLimit, routing::get, Router};
use faststr::FastStr;
use futures::StreamExt;
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
    VolumeEcShardsUnmountResponse, VolumeInfo,
};
use tokio::task::JoinHandle;
use tokio_stream::{wrappers::UnboundedReceiverStream, Stream};
use tonic::{transport::Server as TonicServer, Request, Response, Status, Streaming};
use tower_http::{compression::CompressionLayer, timeout::TimeoutLayer};
use tracing::{debug, error, info};

use crate::{
    errors::{Error, Result},
    operation::Looker,
    proto::save_volume_info,
    rt_spawn,
    storage::{
        api::{delete_handler, get_or_head_handler, post_handler, status_handler, StorageContext},
        erasure_coding::{
            ec_shard_base_filename, find_data_filesize, rebuild_ec_files, rebuild_ecx_file, to_ext,
            write_data_file, write_ec_files, write_index_file_from_ec_index,
            write_sorted_file_from_index, ShardId,
        },
        needle::NeedleMapType,
        store::StoreRef,
        version::Version,
        BUFFER_SIZE_LIMIT,
    },
    util::{
        exit,
        file::file_exists,
        http::{default_handler, favicon_handler},
        parser::parse_addr,
    },
    STOP_INTERVAL,
};

pub struct StorageServer {
    host: FastStr,
    port: u16,
    pub master_node: FastStr,
    pub pulse_seconds: i64,
    pub data_center: FastStr,
    pub rack: FastStr,
    pub store: StoreRef,
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

        let store = StoreRef::new(
            ip,
            port,
            public_url,
            folders,
            max_counts,
            needle_map_type,
            master_node.clone(),
        )
        .await?;

        let storage = StorageServer {
            host: FastStr::new(host),
            port,
            master_node,
            pulse_seconds,
            data_center: FastStr::new(data_center),
            rack: FastStr::new(rack),
            needle_map_type,
            read_redirect,
            store: store.clone(),
            handles: vec![],
            shutdown,
        };

        let addr = format!("{}:{}", host, port + 1).parse()?;

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
        let store = self.store.clone();
        let needle_map_type = self.needle_map_type;
        let read_redirect = self.read_redirect;
        let pulse_seconds = self.pulse_seconds as u64;

        let channel = LoadBalancedChannel::builder(parse_addr::<Error>(&self.master_node)?)
            .channel()
            .await
            .map_err(|err| Error::String(err.to_string()))?;
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
        let addr = format!("{}:{}", self.host, self.port).parse()?;
        let mut shutdown_rx = self.shutdown.new_receiver();

        self.handles.push(rt_spawn(async move {
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
                        let _ = shutdown_rx.recv().await;
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
        }));

        Ok(())
    }
}

async fn start_heartbeat(
    store: StoreRef,
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
                                        store.write().await.set_volume_size_limit(response.volume_size_limit);
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
    store: StoreRef,
    client: &mut HelyimClient<LoadBalancedChannel>,
    pulse_seconds: i64,
    mut shutdown_rx: async_broadcast::Receiver<()>,
) -> Result<Streaming<HeartbeatResponse>> {
    let mut interval = tokio::time::interval(Duration::from_secs(pulse_seconds as u64));

    let request_stream = stream! {
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    match store.write().await.collect_heartbeat().await {
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
    store: StoreRef,
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
            .read()
            .await
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
        let garbage_ratio = self
            .store
            .read()
            .await
            .check_compact_volume(request.volume_id)
            .await?;
        Ok(Response::new(VacuumVolumeCheckResponse { garbage_ratio }))
    }

    async fn vacuum_volume_compact(
        &self,
        request: Request<VacuumVolumeCompactRequest>,
    ) -> StdResult<Response<VacuumVolumeCompactResponse>, Status> {
        let request = request.into_inner();
        debug!("vacuum volume {} compact", request.volume_id);
        self.store
            .read()
            .await
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
        self.store
            .read()
            .await
            .commit_compact_volume(request.volume_id)
            .await?;
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
        self.store
            .read()
            .await
            .commit_cleanup_volume(request.volume_id)
            .await?;
        Ok(Response::new(VacuumVolumeCleanupResponse {}))
    }

    async fn volume_ec_shards_generate(
        &self,
        request: Request<VolumeEcShardsGenerateRequest>,
    ) -> StdResult<Response<VolumeEcShardsGenerateResponse>, Status> {
        let request = request.into_inner();
        match self
            .store
            .read()
            .await
            .find_volume(request.volume_id)
            .await?
        {
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
        for location in self.store.read().await.locations().iter() {
            let ecx_filename = format!("{}{}.ecx", location.read().await.directory, base_filename);
            if file_exists(&ecx_filename)? {
                let base_filename = format!("{}{}", location.read().await.directory, base_filename);
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

        for location in self.store.read().await.locations().iter() {
            let ecx_filename = format!("{}{}.ecx", location.read().await.directory, base_filename);
            if file_exists(&ecx_filename)? {
                found = true;
                base_filename = format!("{}{}", location.read().await.directory, base_filename);
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

        for location in self.store.read().await.locations().iter() {
            let read_dir = fs::read_dir(location.read().await.directory.to_string())?;
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

        if let Some(volume) = self
            .store
            .read()
            .await
            .find_ec_volume(request.volume_id)
            .await
        {
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

        for location in self.store.read().await.locations().iter() {
            if let Some(volume) = location.read().await.find_ec_volume(request.volume_id) {
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

        match self
            .store
            .read()
            .await
            .find_ec_volume(request.volume_id)
            .await
        {
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
