use std::{
    collections::HashMap,
    net::SocketAddr,
    path::MAIN_SEPARATOR,
    pin::Pin,
    result::Result as StdResult,
    time::{Duration, SystemTime},
};

use axum::{extract::DefaultBodyLimit, routing::get, Router};
use faststr::FastStr;
use futures::Stream;
use helyim_proto::filer::{
    filer_server::{Filer as HelyimFiler, FilerServer as HelyimFilerServer},
    AppendToEntryRequest, AppendToEntryResponse, AssignVolumeRequest, AssignVolumeResponse,
    CollectionListRequest, CollectionListResponse, CreateEntryRequest, CreateEntryResponse,
    DeleteCollectionRequest, DeleteCollectionResponse, DeleteEntryRequest, DeleteEntryResponse,
    Entry as PbEntry, KeepConnectedRequest, KeepConnectedResponse, ListEntriesRequest,
    ListEntriesResponse, Location, Locations, LookupDirectoryEntryRequest,
    LookupDirectoryEntryResponse, LookupVolumeRequest, LookupVolumeResponse, UpdateEntryRequest,
    UpdateEntryResponse,
};
use rustix::process::{getgid, getuid};
use tokio::net::TcpListener;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{transport::Server as TonicServer, Request, Response, Status, Streaming};
use tower_http::{compression::CompressionLayer, timeout::TimeoutLayer};
use tracing::{error, info};

use super::http::{delete_handler, post_handler, FilerState};
use crate::{
    errors::Result,
    filer::{
        entry::{entry_attr_to_pb, pb_to_entry_attr, Attr, Entry},
        file_chunk::{compact_file_chunks, find_unused_file_chunks, total_size},
        http::get_or_head_handler,
        Filer, FilerRef,
    },
    operation::{assign, AssignRequest},
    proto::map_error_to_status,
    util::{
        args::FilerOptions, grpc::grpc_port, http::default_handler, sys::exit,
        time::timestamp_to_time,
    },
};

pub struct FilerServer {
    options: FilerOptions,
    filer: FilerRef,
    shutdown: async_broadcast::Sender<()>,
}

impl FilerServer {
    pub async fn new(filer_opts: FilerOptions) -> Result<FilerServer> {
        let (shutdown, mut shutdown_rx) = async_broadcast::broadcast(16);

        let filer = Filer::new(filer_opts.masters.clone());
        let filer_server = FilerServer {
            options: filer_opts.clone(),
            filer: filer.clone(),
            shutdown,
        };
        let addr = format!("{}:{}", filer_opts.ip, grpc_port(filer_opts.port)).parse()?;

        tokio::spawn(async move {
            info!("filer server starting up. binding addr: {addr}");

            if let Err(err) = TonicServer::builder()
                .add_service(HelyimFilerServer::new(FilerGrpcServer {
                    filer,
                    option: filer_opts,
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

        Ok(filer_server)
    }

    pub async fn stop(self) -> Result<()> {
        self.shutdown.broadcast(()).await?;
        Ok(())
    }

    pub async fn start(&mut self) -> Result<()> {
        // http server
        let addr = format!("{}:{}", self.options.ip, self.options.port).parse()?;
        let shutdown_rx = self.shutdown.new_receiver();

        let options = self.options.clone();
        tokio::spawn(start_filer_server(
            FilerState {
                filer: self.filer.clone(),
                options,
            },
            addr,
            shutdown_rx,
        ));
        Ok(())
    }
}

pub async fn start_filer_server(
    state: FilerState,
    addr: SocketAddr,
    mut shutdown: async_broadcast::Receiver<()>,
) {
    let app = Router::new()
        .route(
            "/",
            get(get_or_head_handler)
                .head(get_or_head_handler)
                .post(post_handler)
                .put(post_handler)
                .delete(delete_handler),
        )
        .fallback(default_handler)
        .layer((
            CompressionLayer::new(),
            DefaultBodyLimit::max(1024 * 1024),
            TimeoutLayer::new(Duration::from_secs(10)),
        ))
        .with_state(state);

    info!("filer api server is starting up. binding addr: {addr}");

    match TcpListener::bind(addr).await {
        Ok(listener) => {
            if let Err(err) = axum::serve(listener, app.into_make_service())
                .with_graceful_shutdown(async move {
                    let _ = shutdown.recv().await;
                    info!("filer api server is shutting down gracefully");
                })
                .await
            {
                error!("filer api server is shutting down with error: {err}");
                exit();
            }
        }
        Err(err) => error!("binding directory api address {addr} failed, error: {err}"),
    }
}

pub struct FilerGrpcServer {
    filer: FilerRef,
    option: FilerOptions,
}

#[tonic::async_trait]
impl HelyimFiler for FilerGrpcServer {
    async fn lookup_directory_entry(
        &self,
        request: Request<LookupDirectoryEntryRequest>,
    ) -> StdResult<Response<LookupDirectoryEntryResponse>, Status> {
        let request = request.into_inner();
        let find_entry = self
            .filer
            .find_entry(&format!(
                "{}{MAIN_SEPARATOR}{}",
                request.directory, request.name
            ))
            .await;
        match map_error_to_status(find_entry)? {
            Some(entry) => {
                let attrs = map_error_to_status(entry_attr_to_pb(&entry))?;
                Ok(Response::new(LookupDirectoryEntryResponse {
                    entry: Some(PbEntry {
                        name: request.name,
                        is_directory: entry.is_directory(),
                        attributes: Some(attrs),
                        chunks: entry.chunks,
                        extended: Default::default(),
                    }),
                }))
            }
            None => Err(Status::not_found(format!(
                "entry {} not found under {}",
                request.name, request.directory
            ))),
        }
    }

    type ListEntriesStream =
        Pin<Box<dyn Stream<Item = StdResult<ListEntriesResponse, Status>> + Send>>;

    async fn list_entries(
        &self,
        request: Request<ListEntriesRequest>,
    ) -> StdResult<Response<Self::ListEntriesStream>, Status> {
        let request = request.into_inner();

        let mut limit = request.limit;
        if limit == 0 {
            limit = self.option.dir_listing_limit;
        }
        let mut pagination_limit = 1024 * 256;
        if limit < pagination_limit {
            pagination_limit = limit;
        }

        let mut last_filename = request.start_from_file_name;
        let mut include_last_file = request.inclusive_start_from;

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let filer = self.filer.clone();
        tokio::spawn(async move {
            while limit > 0 {
                match filer
                    .list_directory_entries(
                        &request.directory,
                        &last_filename,
                        include_last_file,
                        pagination_limit,
                    )
                    .await
                {
                    Ok(entries) => {
                        if entries.is_empty() {
                            break;
                        }
                        let entries_len = entries.len();
                        include_last_file = false;
                        for entry in entries {
                            last_filename = entry.full_path.to_string();
                            let attrs = match entry_attr_to_pb(&entry) {
                                Ok(attrs) => attrs,
                                Err(err) => {
                                    error!("convert entry attributes failed, {err}");
                                    break;
                                }
                            };

                            let response = ListEntriesResponse {
                                entry: Some(PbEntry {
                                    name: entry.full_path.clone(),
                                    is_directory: entry.is_directory(),
                                    chunks: entry.chunks,
                                    attributes: Some(attrs),
                                    extended: HashMap::new(),
                                }),
                            };

                            if let Err(err) = tx.send(Ok(response)) {
                                error!("send ListEntriesResponse error: {err}");
                                break;
                            }

                            limit -= 1;
                            if limit == 0 {
                                break;
                            }
                        }

                        if entries_len < pagination_limit as usize {
                            break;
                        }
                    }
                    Err(err) => {
                        error!(
                            "list directory [{}] entries error: {err}",
                            request.directory
                        );
                        if let Err(err) = tx.send(Err(Status::internal(err.to_string()))) {
                            error!("send ListEntriesResponse error: {err}");
                            break;
                        }
                    }
                }
            }
        });
        let stream = UnboundedReceiverStream::new(rx);
        return Ok(Response::new(Box::pin(stream) as Self::ListEntriesStream));
    }

    async fn create_entry(
        &self,
        request: Request<CreateEntryRequest>,
    ) -> StdResult<Response<CreateEntryResponse>, Status> {
        let request = request.into_inner();
        let entry = match request.entry {
            Some(entry) => entry,
            None => return Err(Status::invalid_argument("entry must be set")),
        };
        let full_path = format!("{}{MAIN_SEPARATOR}{}", request.directory, entry.name);
        // TODO: sort entry chunks
        let (chunks, garbages) = compact_file_chunks(&entry.chunks);
        map_error_to_status(self.filer.delete_chunks(garbages.as_ref()))?;

        let attrs = match entry.attributes {
            Some(attrs) => attrs,
            None => {
                return Err(Status::invalid_argument(
                    "can not create entry with empty attributes",
                ))
            }
        };

        let attr = map_error_to_status(pb_to_entry_attr(&attrs))?;
        let create_entry = self
            .filer
            .create_entry(&Entry {
                full_path,
                attr,
                chunks,
            })
            .await;
        map_error_to_status(create_entry)?;
        Ok(Response::new(CreateEntryResponse::default()))
    }

    async fn update_entry(
        &self,
        request: Request<UpdateEntryRequest>,
    ) -> StdResult<Response<UpdateEntryResponse>, Status> {
        let request = request.into_inner();

        let request_entry = match request.entry {
            Some(entry) => entry,
            None => return Err(Status::invalid_argument("entry must be set")),
        };

        let full_path = format!(
            "{}{MAIN_SEPARATOR}{}",
            request.directory, request_entry.name
        );
        let entry = match map_error_to_status(self.filer.find_entry(&full_path).await)? {
            Some(entry) => entry,
            None => {
                return Err(Status::not_found(format!(
                    "entry {} not found under {}",
                    request_entry.name, request.directory
                )))
            }
        };

        let unused_chunks = find_unused_file_chunks(&entry.chunks, &request_entry.chunks);
        let (chunks, garbages) = compact_file_chunks(&entry.chunks);

        let mut new_entry = Entry {
            full_path,
            attr: entry.attr.clone(),
            chunks,
        };

        if let Some(attr) = request_entry.attributes {
            if attr.mtime != 0 {
                new_entry.attr.mtime = map_error_to_status(timestamp_to_time(attr.mtime as u64))?;
            }
            if attr.file_mode != 0 {
                new_entry.attr.mode = attr.file_mode;
            }
            new_entry.attr.uid = attr.uid;
            new_entry.attr.gid = attr.gid;
            new_entry.attr.mime = FastStr::new(&attr.mime);
            new_entry.attr.username = FastStr::new(&attr.user_name);
            new_entry.attr.group_names = attr.group_name.iter().map(FastStr::new).collect();
        }

        if entry == new_entry {
            return Ok(Response::new(UpdateEntryResponse {}));
        }
        if self
            .filer
            .update_entry(Some(&entry), &new_entry)
            .await
            .is_ok()
        {
            map_error_to_status(self.filer.delete_chunks(unused_chunks.as_ref()))?;
            map_error_to_status(self.filer.delete_chunks(garbages.as_ref()))?;
        }
        Ok(Response::new(UpdateEntryResponse {}))
    }

    async fn append_to_entry(
        &self,
        request: Request<AppendToEntryRequest>,
    ) -> StdResult<Response<AppendToEntryResponse>, Status> {
        let mut request = request.into_inner();

        let path = if request.directory.ends_with("/") {
            format!("{}{}", request.directory, request.entry_name)
        } else {
            format!("{}/{}", request.directory, request.entry_name)
        };

        let mut offset = 0;
        let mut entry = match self.filer.find_entry(&path).await {
            Ok(Some(entry)) => {
                offset = total_size(&entry.chunks) as i64;
                entry
            }
            _ => Entry {
                full_path: path,
                attr: Attr {
                    mtime: SystemTime::now(),
                    crtime: SystemTime::now(),
                    mode: 0o644,
                    uid: getuid().as_raw(),
                    gid: getgid().as_raw(),
                    mime: Default::default(),
                    replication: Default::default(),
                    collection: Default::default(),
                    ttl: 0,
                    username: Default::default(),
                    group_names: vec![],
                },
                chunks: vec![],
            },
        };

        for chunk in request.chunks.iter_mut() {
            chunk.offset = offset;
            offset += chunk.size as i64;
        }

        entry.chunks.extend(request.chunks);

        self.filer
            .create_entry(&entry)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        Ok(Response::new(AppendToEntryResponse {}))
    }

    async fn delete_entry(
        &self,
        request: Request<DeleteEntryRequest>,
    ) -> StdResult<Response<DeleteEntryResponse>, Status> {
        let request = request.into_inner();
        let full_path = format!("{}{MAIN_SEPARATOR}{}", request.directory, request.name);
        let delete_entry = self
            .filer
            .delete_entry_meta_and_data(&full_path, request.is_recursive, request.is_delete_data)
            .await;
        map_error_to_status(delete_entry)?;
        Ok(Response::new(DeleteEntryResponse::default()))
    }

    async fn assign_volume(
        &self,
        request: Request<AssignVolumeRequest>,
    ) -> StdResult<Response<AssignVolumeResponse>, Status> {
        let request = request.into_inner();

        let mut ttl_str = String::new();
        if request.ttl_sec > 0 {
            ttl_str = itoa::Buffer::new().format(request.ttl_sec).to_string();
        }

        let mut data_center = request.data_center;
        if data_center.is_empty() {
            data_center = self.option.data_center.to_string();
        }

        let mut rack = request.rack;
        if rack.is_empty() {
            rack = self.option.rack.to_string();
        }

        let assign_request = AssignRequest {
            count: Some(request.count as u64),
            replication: Some(FastStr::new(request.replication)),
            collection: Some(FastStr::new(request.collection)),
            ttl: Some(FastStr::new(ttl_str)),
            data_center: Some(FastStr::new(data_center)),
            rack: Some(FastStr::new(rack)),
            preallocate: None,
            data_node: None,
            writable_volume_count: None,
        };

        let assignment = assign(&self.filer.current_master(), assign_request).await;
        let assignment = map_error_to_status(assignment)?;

        let mut response = AssignVolumeResponse::default();

        if !assignment.error.is_empty() {
            response.error = assignment.error.to_string();
            return Ok(Response::new(response));
        }

        response.file_id = assignment.fid.to_string();
        response.count = assignment.count as i32;
        response.location = Some(Location {
            url: assignment.url.to_string(),
            public_url: assignment.public_url.to_string(),
        });
        Ok(Response::new(response))
    }

    async fn lookup_volume(
        &self,
        request: Request<LookupVolumeRequest>,
    ) -> StdResult<Response<LookupVolumeResponse>, Status> {
        let request = request.into_inner();
        let mut locations = HashMap::new();
        for vid in request.volume_ids {
            let mut locs = vec![];
            match self.filer.master_client.get_locations(&vid) {
                Some(locations) => {
                    for loc in locations.iter() {
                        locs.push(Location {
                            url: loc.url.to_string(),
                            public_url: loc.public_url.to_string(),
                        });
                    }
                }
                None => continue,
            }
            locations.insert(vid, Locations { locations: locs });
        }
        Ok(Response::new(LookupVolumeResponse {
            locations_map: locations,
        }))
    }

    async fn collection_list(
        &self,
        _request: Request<CollectionListRequest>,
    ) -> StdResult<Response<CollectionListResponse>, Status> {
        todo!()
    }

    async fn delete_collection(
        &self,
        _request: Request<DeleteCollectionRequest>,
    ) -> StdResult<Response<DeleteCollectionResponse>, Status> {
        todo!()
    }

    type KeepConnectedStream =
        Pin<Box<dyn Stream<Item = StdResult<KeepConnectedResponse, Status>> + Send>>;

    async fn keep_connected(
        &self,
        _request: Request<Streaming<KeepConnectedRequest>>,
    ) -> StdResult<Response<Self::KeepConnectedStream>, Status> {
        todo!()
    }
}
