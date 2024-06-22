use std::{collections::HashMap, path::MAIN_SEPARATOR, pin::Pin};

use faststr::FastStr;
use futures::Stream;
use helyim_proto::filer::{
    helyim_filer_server::HelyimFiler, AppendToEntryRequest, AppendToEntryResponse,
    AssignVolumeRequest, AssignVolumeResponse, CollectionListRequest, CollectionListResponse,
    CreateEntryRequest, CreateEntryResponse, DeleteCollectionRequest, DeleteCollectionResponse,
    DeleteEntryRequest, DeleteEntryResponse, Entry as PbEntry, KvGetRequest, KvGetResponse,
    KvPutRequest, KvPutResponse, ListEntriesRequest, ListEntriesResponse,
    LookupDirectoryEntryRequest, LookupDirectoryEntryResponse, LookupVolumeRequest,
    LookupVolumeResponse, PingRequest, PingResponse, UpdateEntryRequest, UpdateEntryResponse,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status};
use tracing::error;

use crate::{
    filer::{
        entry::{entry_attr_to_pb, pb_to_entry_attr, Entry},
        file_chunk::{compact_file_chunks, find_unused_file_chunks},
        FilerRef,
    },
    proto::map_error_to_status,
    util::time::timestamp_to_time,
};

#[derive(Clone)]
pub struct FilerOption {
    pub masters: Vec<FastStr>,
    collection: FastStr,
    default_replication: FastStr,
    redirect_on_read: bool,
    data_center: FastStr,
    disable_dir_listing: bool,
    max_mb: u64,
    dir_listing_limit: u32,
}

pub struct FilerGrpcServer {
    filer: FilerRef,
    option: FilerOption,
}

#[tonic::async_trait]
impl HelyimFiler for FilerGrpcServer {
    async fn lookup_directory_entry(
        &self,
        request: Request<LookupDirectoryEntryRequest>,
    ) -> Result<Response<LookupDirectoryEntryResponse>, Status> {
        let request = request.into_inner();
        let find_entry = self
            .filer
            .find_entry(&format!("{}{}", request.directory, request.name));
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
        Pin<Box<dyn Stream<Item = Result<ListEntriesResponse, Status>> + Send>>;

    async fn list_entries(
        &self,
        request: Request<ListEntriesRequest>,
    ) -> Result<Response<Self::ListEntriesStream>, Status> {
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
                match filer.list_directory_entries(
                    &request.directory,
                    &last_filename,
                    include_last_file,
                    pagination_limit,
                ) {
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
    ) -> Result<Response<CreateEntryResponse>, Status> {
        let request = request.into_inner();
        let entry = match request.entry {
            Some(entry) => entry,
            None => return Err(Status::invalid_argument("entry must be set")),
        };
        let full_path = format!("{}{MAIN_SEPARATOR}{}", request.directory, entry.name);
        let (chunks, garbages) = compact_file_chunks(entry.chunks);
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
        let create_entry = self.filer.create_entry(&Entry {
            full_path,
            attr,
            chunks,
        });
        map_error_to_status(create_entry)?;
        Ok(Response::new(CreateEntryResponse::default()))
    }

    async fn update_entry(
        &self,
        request: Request<UpdateEntryRequest>,
    ) -> Result<Response<UpdateEntryResponse>, Status> {
        let request = request.into_inner();

        let request_entry = match request.entry {
            Some(entry) => entry,
            None => return Err(Status::invalid_argument("entry must be set")),
        };

        let full_path = format!(
            "{}{MAIN_SEPARATOR}{}",
            request.directory, request_entry.name
        );
        let entry = match map_error_to_status(self.filer.find_entry(&full_path))? {
            Some(entry) => entry,
            None => return Err(Status::not_found("")),
        };

        let unused_chunks = find_unused_file_chunks(&entry.chunks, &request_entry.chunks);
        let (chunks, garbages) = compact_file_chunks(entry.chunks.clone());

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
        if self.filer.update_entry(Some(&entry), &new_entry).is_ok() {
            map_error_to_status(self.filer.delete_chunks(unused_chunks.as_ref()))?;
            map_error_to_status(self.filer.delete_chunks(garbages.as_ref()))?;
        }
        Ok(Response::new(UpdateEntryResponse {}))
    }

    async fn append_to_entry(
        &self,
        request: Request<AppendToEntryRequest>,
    ) -> Result<Response<AppendToEntryResponse>, Status> {
        todo!()
    }

    async fn delete_entry(
        &self,
        request: Request<DeleteEntryRequest>,
    ) -> Result<Response<DeleteEntryResponse>, Status> {
        let request = request.into_inner();
        let full_path = format!("{}{MAIN_SEPARATOR}{}", request.directory, request.name);
        let delete_entry = self.filer.delete_entry_meta_and_data(
            &full_path,
            request.is_recursive,
            request.is_delete_data,
        );
        map_error_to_status(delete_entry)?;
        Ok(Response::new(DeleteEntryResponse::default()))
    }

    async fn assign_volume(
        &self,
        request: Request<AssignVolumeRequest>,
    ) -> Result<Response<AssignVolumeResponse>, Status> {
        let request = request.into_inner();

        let mut ttl_str = String::new();
        if request.ttl_sec > 0 {
            ttl_str = itoa::Buffer::new().format(request.ttl_sec).to_string();
        }

        let mut data_center = request.data_center;
        if data_center.is_empty() {
            data_center = self.option.data_center.to_string();
        }

        todo!()
    }

    async fn lookup_volume(
        &self,
        request: Request<LookupVolumeRequest>,
    ) -> Result<Response<LookupVolumeResponse>, Status> {
        todo!()
    }

    async fn collection_list(
        &self,
        request: Request<CollectionListRequest>,
    ) -> Result<Response<CollectionListResponse>, Status> {
        todo!()
    }

    async fn delete_collection(
        &self,
        request: Request<DeleteCollectionRequest>,
    ) -> Result<Response<DeleteCollectionResponse>, Status> {
        todo!()
    }

    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        todo!()
    }

    async fn kv_get(
        &self,
        request: Request<KvGetRequest>,
    ) -> Result<Response<KvGetResponse>, Status> {
        todo!()
    }

    async fn kv_put(
        &self,
        request: Request<KvPutRequest>,
    ) -> Result<Response<KvPutResponse>, Status> {
        todo!()
    }
}
