use std::{
    num::ParseIntError,
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_recursion::async_recursion;
use axum::{
    response::{IntoResponse, Response},
    Json,
};
use faststr::FastStr;
use futures::channel::mpsc::{unbounded, TrySendError, UnboundedSender};
use hyper::StatusCode;
use moka::sync::Cache;
use serde_json::json;
use tokio::task::JoinError;
use tracing::{error, info};

use crate::{
    client::{ClientError, MasterClient},
    filer::{
        deletion::loop_processing_deletion,
        entry::{Attr, Entry},
    },
    util::file::{file_name, join_path},
};

pub mod deletion;
pub mod entry;
mod file_chunk;
mod http;
mod server;
pub use server::{start_filer_server, FilerServer};

use crate::util::http::HttpError;

#[async_trait::async_trait]
pub trait FilerStore: Send + Sync {
    fn name(&self) -> &str;
    async fn initialize(&self) -> Result<(), FilerError>;
    async fn insert_entry(&self, entry: &Entry) -> Result<(), FilerError>;
    async fn update_entry(&self, entry: &Entry) -> Result<(), FilerError>;
    async fn find_entry(&self, path: &str) -> Result<Option<Entry>, FilerError>;
    async fn delete_entry(&self, path: &str) -> Result<(), FilerError>;
    async fn list_directory_entries(
        &self,
        dir_path: &str,
        start_filename: &str,
        include_start_file: bool,
        limit: u32,
    ) -> Result<Vec<Entry>, FilerError>;

    fn begin_transaction(&self) -> Result<(), FilerError>;
    fn commit_transaction(&self) -> Result<(), FilerError>;
    fn rollback_transaction(&self) -> Result<(), FilerError>;
}

pub struct Filer {
    store: Option<Box<dyn FilerStore>>,
    directories: Option<Cache<FastStr, Entry>>,
    delete_file_id_tx: UnboundedSender<String>,
    master_client: MasterClient,
}

impl Filer {
    pub fn new(masters: Vec<FastStr>) -> FilerRef {
        let directories = moka::sync::CacheBuilder::new(1000)
            .time_to_live(Duration::from_secs(60 * 60))
            .name("filer-directory-cache")
            .build();
        let master_client = MasterClient::new("filer", masters);
        let (delete_file_id_tx, delete_file_id_rx) = unbounded();

        let filer = Arc::new(Self {
            store: None,
            directories: Some(directories),
            delete_file_id_tx,
            master_client,
        });

        tokio::spawn(loop_processing_deletion(delete_file_id_rx));

        filer
    }

    pub fn current_master(&self) -> String {
        self.master_client.current_master()
    }

    pub fn filer_store(&self) -> Result<&dyn FilerStore, FilerError> {
        match self.store.as_ref() {
            Some(store) => Ok(store.as_ref()),
            None => Err(FilerError::StoreNotInitialized),
        }
    }

    pub async fn keep_connected_to_master(&self) -> Result<(), FilerError> {
        Ok(self.master_client.try_all_masters().await?)
    }

    pub async fn create_entry(&self, entry: &Entry) -> Result<(), FilerError> {
        if entry.full_path == "/" {
            return Ok(());
        }

        let dir_parts: Vec<&str> = entry.full_path.split("/").collect();
        let mut last_directory_entry: Option<Entry> = None;

        for i in 1..dir_parts.len() {
            let dir_path = join_path(&dir_parts, i);
            let dir_entry = match self.get_directory(&dir_path) {
                Some(dir) => {
                    info!("find cached directory: {}", dir_path);
                    Some(dir)
                }
                None => {
                    info!("find uncached directory: {}", dir_path);
                    self.find_entry(&dir_path).await?
                }
            };

            match dir_entry {
                Some(entry) => {
                    if !entry.is_directory() {
                        return Err(FilerError::IsFile(dir_path.to_string()));
                    }

                    self.set_directory(FastStr::new(dir_path), entry.clone());

                    if i == dir_parts.len() - 1 {
                        last_directory_entry = Some(entry);
                    }
                }
                None => {
                    let now = SystemTime::now();
                    let entry = Entry {
                        full_path: dir_path,
                        attr: Attr {
                            mtime: now,
                            crtime: now,
                            mode: 1 << 31 | 0o770,
                            uid: entry.attr.uid,
                            gid: entry.attr.gid,
                            mime: FastStr::empty(),
                            replication: FastStr::empty(),
                            collection: FastStr::empty(),
                            ttl: 0,
                            username: FastStr::empty(),
                            group_names: vec![],
                        },
                        chunks: Vec::new(),
                    };
                    let store = self.filer_store()?;
                    store.insert_entry(&entry).await?;
                    // FIXME: is there need to find entry?
                }
            }
        }

        if last_directory_entry.is_none() {
            return Err(FilerError::NotFound);
        }

        let old_entry = self.find_entry(&entry.full_path).await?;
        match old_entry {
            Some(ref old_entry) => {
                if let Err(err) = self.update_entry(Some(old_entry), entry).await {
                    error!("update entry: {}, {err}", entry.full_path);
                    return Err(err);
                }
            }
            None => {
                let store = self.filer_store()?;
                if let Err(err) = store.insert_entry(entry).await {
                    error!("insert entry: {}, {err}", entry.full_path);
                    return Err(err);
                }
            }
        }

        self.delete_chunks_if_not_new(old_entry.as_ref(), Some(entry))
    }

    pub async fn update_entry(
        &self,
        old_entry: Option<&Entry>,
        entry: &Entry,
    ) -> Result<(), FilerError> {
        if let Some(old_entry) = old_entry {
            if old_entry.is_directory() && !entry.is_directory() {
                error!("existing {0} is a directory", entry.path());
                return Err(FilerError::IsDirectory(entry.full_path.clone()));
            }
            if !old_entry.is_directory() && entry.is_directory() {
                error!("existing {0} is a file", entry.path());
                return Err(FilerError::IsFile(entry.full_path.clone()));
            }
        }
        let store = self.filer_store()?;
        store.update_entry(entry).await
    }

    pub async fn find_entry(&self, path: &str) -> Result<Option<Entry>, FilerError> {
        let time = SystemTime::now();
        if path == "/" {
            return Ok(Some(Entry {
                full_path: path.to_string(),
                attr: Attr::root_path(time),
                chunks: vec![],
            }));
        }
        let store = self.filer_store()?;
        store.find_entry(path).await
        // TODO: sort entry chunks
    }

    pub async fn list_directory_entries(
        &self,
        path: &str,
        start_filename: &str,
        inclusive: bool,
        limit: u32,
    ) -> Result<Vec<Entry>, FilerError> {
        let mut path = path;
        if path.ends_with('/') && path.len() > 1 {
            path = &path[..path.len() - 1];
        }
        let store = self.filer_store()?;
        store
            .list_directory_entries(path, start_filename, inclusive, limit)
            .await
    }

    #[async_recursion]
    pub async fn delete_entry_meta_and_data(
        &self,
        path: &str,
        is_recursive: bool,
        should_delete_chunks: bool,
    ) -> Result<(), FilerError> {
        let find_entry = self.find_entry(path).await?;
        if let Some(entry) = find_entry {
            if entry.is_directory() {
                let mut limit = 1;
                if is_recursive {
                    limit = i32::MAX;
                }
                let mut last_filename = String::new();
                let include_last_file = false;

                while limit > 0 {
                    let entries = self
                        .list_directory_entries(path, &last_filename, include_last_file, 1024)
                        .await?;
                    if entries.is_empty() {
                        break;
                    }

                    if is_recursive {
                        for entry in entries.iter() {
                            if let Some(filename) = file_name(entry.path()) {
                                last_filename = filename;
                            }
                            self.delete_entry_meta_and_data(
                                entry.path(),
                                is_recursive,
                                should_delete_chunks,
                            )
                            .await?;
                            limit -= 1;
                            if limit <= 0 {
                                break;
                            }
                        }
                    }

                    if entries.len() < 1024 {
                        break;
                    }
                }

                self.delete_directory(path);
            }

            if should_delete_chunks {
                self.delete_chunks(entry.chunks.as_ref())?;
            }

            if path == "/" {
                return Ok(());
            }

            let store = self.filer_store()?;
            return store.delete_entry(path).await;
        }

        Ok(())
    }

    pub fn delete_directory(&self, path: &str) {
        if path == "/" {
            return;
        }
        if let Some(cache) = self.directories.as_ref() {
            cache.remove(path);
        }
    }

    pub fn get_directory(&self, path: &str) -> Option<Entry> {
        match self.directories.as_ref() {
            Some(cache) => cache.get(path),
            None => None,
        }
    }

    pub fn set_directory(&self, path: FastStr, entry: Entry) {
        if let Some(cache) = self.directories.as_ref() {
            cache.insert(path, entry);
        }
    }
}

pub type FilerRef = Arc<Filer>;

#[derive(thiserror::Error, Debug)]
pub enum FilerError {
    #[error("Master client error: {0}")]
    MasterClient(#[from] ClientError),
    #[error("Existing {0} is a directory")]
    IsDirectory(String),
    #[error("Existing {0} is a file")]
    IsFile(String),
    #[error("Not found")]
    NotFound,
    #[error("Filer store is not initialized.")]
    StoreNotInitialized,
    #[error("Try send error: {0}")]
    TrySend(#[from] TrySendError<String>),
    #[error("Http error: {0}")]
    Http(#[from] HttpError),
    #[error("Rustix errno: {0}")]
    Errno(#[from] rustix::io::Errno),
    #[error("Parse int error: {0}")]
    ParseInt(#[from] ParseIntError),

    #[error("Tokio task join error: {0}")]
    TokioJoin(#[from] JoinError),

    #[error("Serde Json error: {0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error("Io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("error: {0}")]
    Box(#[from] Box<dyn std::error::Error + Sync + Send>),
}

unsafe impl Send for FilerError {}

impl IntoResponse for FilerError {
    fn into_response(self) -> Response {
        let status_code = match self {
            FilerError::NotFound => StatusCode::NOT_FOUND,
            FilerError::IsDirectory(_) | FilerError::IsFile(_) => StatusCode::CONFLICT,
            FilerError::StoreNotInitialized => StatusCode::SERVICE_UNAVAILABLE,
            FilerError::MasterClient(_) => StatusCode::BAD_GATEWAY,
            _ => StatusCode::BAD_REQUEST,
        };
        let error = json!({
            "error": self.to_string()
        });
        let response = (status_code, Json(error));
        response.into_response()
    }
}
