use std::{
    fs,
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_recursion::async_recursion;
use async_trait::async_trait;
use axum::{
    response::{IntoResponse, Response},
    Json,
};
use faststr::FastStr;
use futures::channel::mpsc::{unbounded, TrySendError, UnboundedSender};
use helyim_proto::filer::FileId;
use hyper::StatusCode;
use moka::sync::Cache;
use serde_json::json;
use toml::Value;
use tracing::error;

use self::redis::redis_store::RedisStore;
use crate::{
    client::{ClientError, MasterClient},
    filer::{
        deletion::loop_processing_deletion,
        entry::{Attr, Entry},
    },
    util::file::file_name,
};

mod redis;

pub mod api;
pub mod deletion;
pub mod entry;
pub mod file_chunks;
pub mod server;
pub mod stream;
pub mod util;
pub mod write;

pub use server::FilerSever;

#[async_trait]
pub trait FilerStore: Send + Sync {
    fn name(&self) -> &str;
    fn initialize(&mut self) -> Result<(), FilerError>;
    async fn insert_entry(&self, entry: &Entry) -> Result<(), FilerError>;
    async fn update_entry(&self, entry: &Entry) -> Result<(), FilerError>;
    async fn find_entry(&self, path: &str) -> Result<Option<Entry>, FilerError>;
    async fn delete_entry(&self, path: &str) -> Result<(), FilerError>;
    async fn list_directory_entries(
        &self,
        dir_path: &str,
        start_filename: &str,
        include_start_file: bool,
        limit: usize,
    ) -> Result<Vec<Entry>, FilerError>;

    fn begin_transaction(&self) -> Result<(), FilerError>;
    fn commit_transaction(&self) -> Result<(), FilerError>;
    fn rollback_transaction(&self) -> Result<(), FilerError>;
}

pub struct Filer {
    store: Option<Box<dyn FilerStore>>,
    directories: Option<Cache<FastStr, Entry>>,
    delete_file_id_tx: UnboundedSender<Option<FileId>>,
    master_client: Arc<MasterClient>,
}

impl Filer {
    pub fn new(masters: Vec<FastStr>) -> Result<FilerRef, FilerError> {
        let directories = moka::sync::CacheBuilder::new(1000)
            .time_to_live(Duration::from_secs(60 * 60))
            .name("filer-directory-cache")
            .build();
        let master_client = MasterClient::new("filer", masters);
        let (delete_file_id_tx, delete_file_id_rx) = unbounded();

        let mut filer = Self {
            store: None,
            directories: Some(directories),
            delete_file_id_tx,
            master_client: Arc::new(master_client),
        };

        filer.initialize()?;

        tokio::spawn(loop_processing_deletion(delete_file_id_rx));

        Ok(Arc::new(filer))
    }

    pub fn initialize(&mut self) -> Result<(), FilerError> {
        let config_paths = vec![
            "./config.toml",
            "~/.seaweedfs/config.toml",
            "/etc/seaweedfs/config.toml",
        ];

        for config_path in config_paths {
            if Path::new(config_path).exists() {
                println!("{config_path}");
                let content = fs::read_to_string(config_path).expect("Failed to read file");
                let value: Value = content.parse().expect("Failed to parse TOML");
                if let Some(redis) = value.get("redis") {
                    if let Some(addr) = redis.get("addr") {
                        let mut store = RedisStore::new(addr.as_str().unwrap());
                        store.initialize()?;

                        self.store = Some(Box::new(store));

                        return Ok(());
                    }
                }
            }
        }

        Err(FilerError::String("config file err".to_string()))
    }

    pub async fn current_master(&self) -> FastStr {
        self.master_client.current_master().await.clone()
    }

    pub fn filer_store(&self) -> Result<&dyn FilerStore, FilerError> {
        match self.store.as_ref() {
            Some(store) => Ok(store.as_ref()),
            None => Err(FilerError::StoreNotInitialized),
        }
    }

    pub async fn keep_connected_to_master(&self) {
        self.master_client.keep_connected_to_master().await
    }

    pub async fn update_entry(
        &self,
        old_entry: Option<&Entry>,
        entry: &Entry,
    ) -> Result<(), FilerError> {
        if let Some(old_entry) = old_entry {
            if old_entry.is_directory() && !entry.is_directory() {
                error!("existing {0} is a directory", entry.path());
                return Err(FilerError::IsDirectory(entry.path.clone()));
            }
            if !old_entry.is_directory() && entry.is_directory() {
                error!("existing {0} is a file", entry.path());
                return Err(FilerError::IsFile(entry.path.clone()));
            }
        }

        self.filer_store()?.update_entry(entry).await
    }

    pub async fn find_entry(&self, path: &str) -> Result<Option<Entry>, FilerError> {
        let time = SystemTime::now();
        if path == "/" {
            return Ok(Some(Entry {
                path: FastStr::new(path),
                attr: Attr::root_path(time),
                chunks: vec![],
            }));
        }
        let result = self.filer_store()?.find_entry(path);
        result.await
    }

    pub async fn list_directory_entries(
        &self,
        path: &str,
        start_filename: &str,
        inclusive: bool,
        limit: usize,
    ) -> Result<Vec<Entry>, FilerError> {
        let mut path = path;
        if path.starts_with('/') && path.len() > 1 {
            path = &path[..path.len() - 1];
        }
        let result =
            self.filer_store()?
                .list_directory_entries(path, start_filename, inclusive, limit);
        result.await
    }

    #[async_recursion]
    pub async fn delete_entry_meta_and_data(
        &self,
        path: &str,
        is_recursive: bool,
        should_delete_chunks: bool,
    ) -> Result<(), FilerError> {
        if let Some(entry) = self.find_entry(path).await? {
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
                            last_filename = file_name(entry.path());
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
                self.delete_chunks(path, entry.chunks)?;
            }

            if path == "/" {
                return Ok(());
            }

            return self.filer_store()?.delete_entry(path).await;
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
    #[error("file store err: {0}")]
    FileStoreErr(FastStr),
    #[error("init FilerStore failed: {0}")]
    InitErr(FastStr),
    #[error("Master client error: {0}")]
    MasterClient(#[from] ClientError),
    #[error("Existing {0} is a directory")]
    IsDirectory(FastStr),
    #[error("Existing {0} is a file")]
    IsFile(FastStr),
    #[error("Reqwest error: {0}")]
    Request(#[from] reqwest::Error),
    #[error("Filer store is not initialized.")]
    StoreNotInitialized,
    #[error("Raw Filer error: {0}")]
    String(String),
    #[error("serialization error")]
    SerializationErr,
    #[error("Try send error: {0}")]
    TrySend(#[from] TrySendError<Option<FileId>>),
}

impl From<FilerError> for tonic::Status {
    fn from(value: FilerError) -> Self {
        tonic::Status::internal(value.to_string())
    }
}

impl From<nom::Err<nom::error::Error<&str>>> for FilerError {
    fn from(value: nom::Err<nom::error::Error<&str>>) -> Self {
        Self::String(value.to_string())
    }
}

impl IntoResponse for FilerError {
    fn into_response(self) -> Response {
        let error = self.to_string();
        let error = json!({
            "error": error
        });
        let response = (StatusCode::BAD_REQUEST, Json(error));
        response.into_response()
    }
}

// #[test]
// fn test_new_filer() -> Result<(), FilerError> {
//     let _ = Filer::new(vec!["127.0.0.1:9333".into()])?;
//     Ok(())
// }
