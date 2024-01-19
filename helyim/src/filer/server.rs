use std::{
    result::Result as StdResult,
    sync::Arc, pin::Pin, collections::HashMap,
};

use faststr::FastStr;
use helyim_proto::filer::{
    helyim_filer_server::{HelyimFilerServer, HelyimFiler}, 
    LookupDirectoryEntryRequest, LookupDirectoryEntryResponse, ListEntriesRequest, ListEntriesResponse,
    CreateEntryRequest, CreateEntryResponse, UpdateEntryRequest, UpdateEntryResponse, AppendToEntryRequest,
    AppendToEntryResponse, DeleteEntryRequest, DeleteEntryResponse, AssignVolumeRequest, AssignVolumeResponse,
    LookupVolumeRequest, LookupVolumeResponse, CollectionListRequest, CollectionListResponse, DeleteCollectionRequest,
    DeleteCollectionResponse, PingRequest, PingResponse, KvGetRequest, KvGetResponse, KvPutRequest, KvPutResponse, Entry,
};
use tokio_stream::Stream;
use tonic::{transport::Server as TonicServer, Request, Response, Status};
use tracing::info;

use crate::{
    errors::Result,
    rt_spawn,
    util::{args::FilerOptions, grpc::grpc_port, file::new_full_path},
};

use super::{Filer, FilerRef, entry::{entry_attribute_to_pb, entry_to_pb}};

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
        let filer = Filer::new(options.masters.clone());

        // let addr = format!("{}:{}", options.ip, grpc_port(options.port)).parse()?;

        let filer_server = Self {
            options,
            filer: filer.clone(),
            current_master: FastStr::empty(),
            seed_master_nodes: Vec::new(),
            shutdown,
        };

        Ok(filer_server)
    }

    pub async fn stop(self) -> Result<()> {
        self.shutdown.broadcast(()).await?;
        Ok(())
    }

    pub async fn start(&mut self) -> Result<()> {
        todo!()
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
    ) -> StdResult<
        Response<LookupDirectoryEntryResponse>,
        Status,
    > {
        let request: LookupDirectoryEntryRequest = request.into_inner();
        let entry = self.filer.find_entry(&new_full_path(
            request.directory.as_str(), request.name.as_str(),
        )).await.map_err(|err| {
            Status::internal(err.to_string())
        })?;

        if let Some(entry) = entry {
            return Ok(Response::new(LookupDirectoryEntryResponse {entry: Some(entry_to_pb(&entry))}));
        }

        Ok(Response::new(LookupDirectoryEntryResponse {entry: None}))
    }
    /// Server streaming response type for the ListEntries method.
    type ListEntriesStream = 
        Pin<Box<dyn Stream<Item = StdResult<ListEntriesResponse, Status>> + Send>>;
        
    async fn list_entries(
        &self,
        request: Request<ListEntriesRequest>,
    ) -> StdResult<
        Response<Self::ListEntriesStream>,
        Status,
    > {
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
    ) -> StdResult<
        Response<CreateEntryResponse>,
        Status,
    > {
        todo!()
    }
    async fn update_entry(
        &self,
        request: Request<UpdateEntryRequest>,
    ) -> StdResult<
        Response<UpdateEntryResponse>,
        Status,
    > {
        todo!()
    }
    async fn append_to_entry(
        &self,
        request: Request<AppendToEntryRequest>,
    ) -> StdResult<
        Response<AppendToEntryResponse>,
        Status,
    > {
        todo!()
    }
    async fn delete_entry(
        &self,
        request: Request<DeleteEntryRequest>,
    ) -> StdResult<
        Response<DeleteEntryResponse>,
        Status,
    > {
        todo!()
    }
    async fn assign_volume(
        &self,
        request: Request<AssignVolumeRequest>,
    ) -> StdResult<
        Response<AssignVolumeResponse>,
        Status,
    > {
        todo!()
    }
    async fn lookup_volume(
        &self,
        request: Request<LookupVolumeRequest>,
    ) -> StdResult<
        Response<LookupVolumeResponse>,
        Status,
    > {
        todo!()
    }
    async fn collection_list(
        &self,
        request: Request<CollectionListRequest>,
    ) -> StdResult<
        Response<CollectionListResponse>,
        Status,
    > {
        todo!()
    }
    async fn delete_collection(
        &self,
        request: Request<DeleteCollectionRequest>,
    ) -> StdResult<
        Response<DeleteCollectionResponse>,
        Status,
    > {
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


