use std::{net::SocketAddr, pin::Pin, result::Result as StdResult, sync::Arc, time::Duration};

use axum::{extract::DefaultBodyLimit, routing::get, Router};
use faststr::FastStr;
use helyim_proto::filer::{
    helyim_filer_server::{HelyimFiler, HelyimFilerServer},
    AppendToEntryRequest, AppendToEntryResponse, AssignVolumeRequest, AssignVolumeResponse,
    CollectionListRequest, CollectionListResponse, CreateEntryRequest, CreateEntryResponse,
    DeleteCollectionRequest, DeleteCollectionResponse, DeleteEntryRequest, DeleteEntryResponse,
    KvGetRequest, KvGetResponse, KvPutRequest, KvPutResponse, ListEntriesRequest,
    ListEntriesResponse, LookupDirectoryEntryRequest, LookupDirectoryEntryResponse,
    LookupVolumeRequest, LookupVolumeResponse, PingRequest, PingResponse, UpdateEntryRequest,
    UpdateEntryResponse,
};
use tokio_stream::Stream;
use tonic::{transport::Server as TonicServer, Request, Response, Status};
use tower_http::{compression::CompressionLayer, timeout::TimeoutLayer};
use tracing::{error, info};

use super::{
    api::{delete_handler, get_or_head_handler, post_handler},
    entry::entry_to_pb,
    Filer, FilerError, FilerRef,
};
use crate::{
    errors::Result,
    filer::api::FilerState,
    operation::list_master,
    rt_spawn,
    util::{
        args::FilerOptions,
        file::new_full_path,
        grpc::grpc_port,
        http::{default_handler, favicon_handler},
        sys::exit,
    },
};

pub struct FilerSever {
    pub options: Arc<FilerOptions>,
    pub filer: FilerRef,
    pub current_master: FastStr,
    pub seed_master_nodes: Vec<FastStr>,

    shutdown: async_broadcast::Sender<()>,
}

impl FilerSever {
    pub async fn new(filer_opts: FilerOptions) -> Result<Self> {
        let (shutdown, mut shutdown_rx) = async_broadcast::broadcast(16);

        let options = Arc::new(filer_opts);
        let filer = Filer::new(options.master_server.clone())?;

        let addr = format!("{}:{}", options.ip, grpc_port(options.port)).parse()?;

        let filer_server = Self {
            options,
            filer: filer.clone(),
            current_master: FastStr::empty(),
            seed_master_nodes: Vec::new(),
            shutdown,
        };

        rt_spawn(async move {
            info!("filer grpc server starting up. binding addr: {addr}");
            if let Err(err) = TonicServer::builder()
                .add_service(HelyimFilerServer::new(FilerGrpcServer { filer }))
                .serve_with_shutdown(addr, async {
                    let _ = shutdown_rx.recv().await;
                })
                .await
            {
                error!("filer grpc server starting failed, {err}");
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
        let filer = self.filer.clone();
        let read_redirect = self.options.redirect_on_read;
        let disable_dir_listing = self.options.disable_dir_listing;

        // self.filer.keep_connected_to_master().await?;

        self.update_masters().await?;

        let ctx = FilerState {
            filer,
            read_redirect,
            disable_dir_listing,
        };

        let addr = format!("{}:{}", self.options.ip, self.options.port).parse()?;
        let shutdown_rx = self.shutdown.new_receiver();

        rt_spawn(start_filer_server(ctx, addr, shutdown_rx));

        Ok(())
    }
}

impl FilerSever {
    async fn update_masters(&mut self) -> StdResult<(), FilerError> {
        let cluster_status = list_master(&self.options.master_server[0]).await?;
        self.current_master = FastStr::new(cluster_status.leader);
        self.seed_master_nodes = cluster_status.peers;

        Ok(())
    }
}

async fn start_filer_server(
    ctx: FilerState,
    addr: SocketAddr,
    mut shutdown: async_broadcast::Receiver<()>,
) {
    let app = Router::new()
        .route("/", get(default_handler))
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
struct FilerGrpcServer {
    filer: FilerRef,
}

#[tonic::async_trait]
impl HelyimFiler for FilerGrpcServer {
    async fn lookup_directory_entry(
        &self,
        request: Request<LookupDirectoryEntryRequest>,
    ) -> StdResult<Response<LookupDirectoryEntryResponse>, Status> {
        let request: LookupDirectoryEntryRequest = request.into_inner();
        let entry = self
            .filer
            .find_entry(&new_full_path(
                request.directory.as_str(),
                request.name.as_str(),
            ))
            .await
            .map_err(|err| Status::internal(err.to_string()))?;

        if let Some(entry) = entry {
            return Ok(Response::new(LookupDirectoryEntryResponse {
                entry: Some(entry_to_pb(&entry)),
            }));
        }

        Ok(Response::new(LookupDirectoryEntryResponse { entry: None }))
    }
    /// Server streaming response type for the ListEntries method.
    type ListEntriesStream =
        Pin<Box<dyn Stream<Item = StdResult<ListEntriesResponse, Status>> + Send>>;

    async fn list_entries(
        &self,
        request: Request<ListEntriesRequest>,
    ) -> StdResult<Response<Self::ListEntriesStream>, Status> {
        // let request: LookupDirectoryEntryRequest = request.into_inner();
        // let entries = self.filer.list_directory_entries(
        //     &request.directory,
        //     &request.name,
        //     true,
        //     100
        // ).await.map_err(|err| {
        //     Status::internal(err.to_string())
        // })?;

        // let mut re_entries: Vec<Entry> = Vec::new();

        // entries.into_iter().for_each(|entry| {
        //     re_entries.push(entry_to_pb(&entry));
        // });

        // Ok(Response::new(LookupDirectoryEntryResponse {entries: re_entries}))
        todo!()
    }
    async fn create_entry(
        &self,
        request: Request<CreateEntryRequest>,
    ) -> StdResult<Response<CreateEntryResponse>, Status> {
        todo!()
    }
    async fn update_entry(
        &self,
        request: Request<UpdateEntryRequest>,
    ) -> StdResult<Response<UpdateEntryResponse>, Status> {
        todo!()
    }
    async fn append_to_entry(
        &self,
        request: Request<AppendToEntryRequest>,
    ) -> StdResult<Response<AppendToEntryResponse>, Status> {
        todo!()
    }
    async fn delete_entry(
        &self,
        request: Request<DeleteEntryRequest>,
    ) -> StdResult<Response<DeleteEntryResponse>, Status> {
        todo!()
    }
    async fn assign_volume(
        &self,
        request: Request<AssignVolumeRequest>,
    ) -> StdResult<Response<AssignVolumeResponse>, Status> {
        todo!()
    }
    async fn lookup_volume(
        &self,
        request: Request<LookupVolumeRequest>,
    ) -> StdResult<Response<LookupVolumeResponse>, Status> {
        todo!()
    }
    async fn collection_list(
        &self,
        request: Request<CollectionListRequest>,
    ) -> StdResult<Response<CollectionListResponse>, Status> {
        todo!()
    }
    async fn delete_collection(
        &self,
        request: Request<DeleteCollectionRequest>,
    ) -> StdResult<Response<DeleteCollectionResponse>, Status> {
        todo!()
    }
    async fn ping(
        &self,
        request: Request<PingRequest>,
    ) -> StdResult<Response<PingResponse>, Status> {
        todo!()
    }
    async fn kv_get(
        &self,
        request: Request<KvGetRequest>,
    ) -> StdResult<Response<KvGetResponse>, Status> {
        todo!()
    }
    async fn kv_put(
        &self,
        request: Request<KvPutRequest>,
    ) -> StdResult<Response<KvPutResponse>, Status> {
        todo!()
    }
}
