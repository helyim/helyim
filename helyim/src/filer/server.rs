use std::{net::SocketAddr, pin::Pin, result::Result as StdResult, sync::Arc, time::Duration};

use axum::{
    extract::{DefaultBodyLimit, State}, response::Html, routing::get, Router
};
use futures::Stream;
use helyim_proto::filer::{
    helyim_filer_server::{HelyimFiler, HelyimFilerServer}, AppendToEntryRequest, AppendToEntryResponse,
    AssignVolumeRequest, AssignVolumeResponse, CollectionListRequest, CollectionListResponse,
    CreateEntryRequest, CreateEntryResponse, DeleteCollectionRequest, DeleteCollectionResponse,
    DeleteEntryRequest, DeleteEntryResponse, KvGetRequest, KvGetResponse, KvPutRequest,
    KvPutResponse, ListEntriesRequest, ListEntriesResponse, LookupDirectoryEntryRequest,
    LookupDirectoryEntryResponse, LookupVolumeRequest, LookupVolumeResponse, PingRequest,
    PingResponse, UpdateEntryRequest, UpdateEntryResponse,
};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tonic::{transport::Server as TonicServer, Request, Response, Status};
use tower_http::{compression::CompressionLayer, timeout::TimeoutLayer};
use tracing::{error, info};

use super::FilerState;
use crate::{errors::Result, util::{args::FilerOptions, grpc::grpc_port, http::default_handler, sys::exit}};

pub struct FilerServer {
    pub options: Arc<FilerOptions>,

    shutdown: async_broadcast::Sender<()>,
}

impl FilerServer {
    pub async fn new(filer_opts: FilerOptions) -> Result<FilerServer> {
        let (shutdown, mut shutdown_rx) = async_broadcast::broadcast(16);
        
        let options = Arc::new(filer_opts);

        let filer = FilerServer {
            options: options.clone(),
            shutdown,
        };
        let addr = format!("{}:{}", options.ip, grpc_port(options.port)).parse()?;

        tokio::spawn(async move {
            info!("filer server starting up. binding addr: {addr}");
            if let Err(err) = TonicServer::builder()
                .add_service(HelyimFilerServer::new(
                    FilerGrpcServer
                ))
                .serve_with_shutdown(addr, async {
                    let _ = shutdown_rx.recv().await;
                })
                .await
            {
                error!("volume grpc server starting failed, {err}");
                exit();
            }
        });

        Ok(filer)
    }

    pub async fn stop(self) -> Result<()> {
        self.shutdown.broadcast(()).await?;
        Ok(())
    }

    pub async fn start(&mut self) -> Result<()> {
        // http server
        let addr = format!("{}:{}", self.options.ip, self.options.port).parse()?;
        let shutdown_rx = self.shutdown.new_receiver();
        tokio::spawn(start_filer_server(FilerState, addr, shutdown_rx));
        Ok(())
    }
}

async fn start_filer_server(
    _state: FilerState,
    addr: SocketAddr,
    mut shutdown: async_broadcast::Receiver<()>,
) {
    let app = Router::new()
        .route("/", get(root_handler).post(root_handler))
        .route("/healthz", get(healthz_handler).post(healthz_handler))
        .fallback(default_handler)
        .layer((
            CompressionLayer::new(),
            DefaultBodyLimit::max(1024 * 1024),
            TimeoutLayer::new(Duration::from_secs(10)),
        ))
        .with_state(FilerState);

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

async fn root_handler(State(_state): State<FilerState>) -> Html<&'static str> {
    Html("root")
}

async fn healthz_handler(State(_state): State<FilerState>) -> Html<&'static str> {
    Html("healthz")
}

#[derive(Serialize, Deserialize)]
pub struct Placeholder;

pub struct FilerGrpcServer;

#[tonic::async_trait]
impl HelyimFiler for FilerGrpcServer {
    async fn lookup_directory_entry(
        &self,
        _request: Request<LookupDirectoryEntryRequest>,
    ) -> StdResult<Response<LookupDirectoryEntryResponse>, Status> {
        // Implement the function here
        todo!()
    }

    type ListEntriesStream =
        Pin<Box<dyn Stream<Item = StdResult<ListEntriesResponse, Status>> + Send + Sync + 'static>>;
    async fn list_entries(
        &self,
        _request: Request<ListEntriesRequest>,
    ) -> StdResult<Response<Self::ListEntriesStream>, Status> {
        // Implement the function here
        todo!()
    }

    async fn create_entry(
        &self,
        _request: Request<CreateEntryRequest>,
    ) -> StdResult<Response<CreateEntryResponse>, Status> {
        // Implement the function here
        todo!()
    }

    async fn update_entry(
        &self,
        _request: Request<UpdateEntryRequest>,
    ) -> StdResult<Response<UpdateEntryResponse>, Status> {
        // Implement the function here
        todo!()
    }

    async fn append_to_entry(
        &self,
        _request: Request<AppendToEntryRequest>,
    ) -> StdResult<Response<AppendToEntryResponse>, Status> {
        // Implement the function here
        todo!()
    }

    async fn delete_entry(
        &self,
        _request: Request<DeleteEntryRequest>,
    ) -> StdResult<Response<DeleteEntryResponse>, Status> {
        // Implement the function here
        todo!()
    }

    async fn assign_volume(
        &self,
        _request: Request<AssignVolumeRequest>,
    ) -> StdResult<Response<AssignVolumeResponse>, Status> {
        // Implement the function here
        todo!()
    }

    async fn lookup_volume(
        &self,
        _request: Request<LookupVolumeRequest>,
    ) -> StdResult<Response<LookupVolumeResponse>, Status> {
        // Implement the function here
        todo!()
    }

    async fn collection_list(
        &self,
        _request: Request<CollectionListRequest>,
    ) -> StdResult<Response<CollectionListResponse>, Status> {
        // Implement the function here
        todo!()
    }

    async fn delete_collection(
        &self,
        _request: Request<DeleteCollectionRequest>,
    ) -> StdResult<Response<DeleteCollectionResponse>, Status> {
        // Implement the function here
        todo!()
    }

    async fn ping(
        &self,
        _request: Request<PingRequest>,
    ) -> StdResult<Response<PingResponse>, Status> {
        // Implement the function here
        todo!()
    }

    async fn kv_get(
        &self,
        _request: Request<KvGetRequest>,
    ) -> StdResult<Response<KvGetResponse>, Status> {
        // Implement the function here
        todo!()
    }

    async fn kv_put(
        &self,
        _request: Request<KvPutRequest>,
    ) -> StdResult<Response<KvPutResponse>, Status> {
        // Implement the function here
        todo!()
    }
}
