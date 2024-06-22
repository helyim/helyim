use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use faststr::FastStr;
use futures::channel::mpsc::{unbounded, TrySendError, UnboundedSender};
use helyim_proto::filer::FileId;
use moka::sync::Cache;
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
mod server;

pub trait FilerStore: Send + Sync {
    fn name(&self) -> &str;
    fn initialize(&self) -> Result<(), FilerError>;
    fn insert_entry(&self, entry: &Entry) -> Result<(), FilerError>;
    fn update_entry(&self, entry: &Entry) -> Result<(), FilerError>;
    fn find_entry(&self, path: &str) -> Result<Option<Entry>, FilerError>;
    fn delete_entry(&self, path: &str) -> Result<(), FilerError>;
    fn list_directory_entries(
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
    delete_file_id_tx: UnboundedSender<Option<FileId>>,
    master_client: MasterClient,
    // dir_buckets_path: FastStr,
    // fsync_buckets: Vec<FastStr>,
    // buckets: FilerBuckets,
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

    pub fn create_entry(&self, entry: &Entry) -> Result<(), FilerError> {
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
                    self.find_entry(&dir_path)?
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
                    self.filer_store()?.insert_entry(&entry)?;
                    // FIXME: is there need to find entry?
                }
            }
        }

        if last_directory_entry.is_none() {
            return Err(FilerError::NotFound);
        }

        let old_entry = self.find_entry(&entry.full_path)?;
        match old_entry {
            Some(ref old_entry) => {
                if let Err(err) = self.update_entry(Some(old_entry), entry) {
                    error!("update entry: {}, {err}", entry.full_path);
                    return Err(err);
                }
            }
            None => {
                if let Err(err) = self.filer_store()?.insert_entry(entry) {
                    error!("insert entry: {}, {err}", entry.full_path);
                    return Err(err);
                }
            }
        }

        self.delete_chunks_if_not_new(old_entry.as_ref(), Some(entry))
    }

    pub fn update_entry(&self, old_entry: Option<&Entry>, entry: &Entry) -> Result<(), FilerError> {
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
        self.filer_store()?.update_entry(entry)
    }

    pub fn find_entry(&self, path: &str) -> Result<Option<Entry>, FilerError> {
        let time = SystemTime::now();
        if path == "/" {
            return Ok(Some(Entry {
                full_path: path.to_string(),
                attr: Attr::root_path(time),
                chunks: vec![],
            }));
        }
        self.filer_store()?.find_entry(path)
    }

    pub fn list_directory_entries(
        &self,
        path: &str,
        start_filename: &str,
        inclusive: bool,
        limit: u32,
    ) -> Result<Vec<Entry>, FilerError> {
        let mut path = path;
        if path.starts_with('/') && path.len() > 1 {
            path = &path[..path.len() - 1];
        }
        self.filer_store()?
            .list_directory_entries(path, start_filename, inclusive, limit)
    }

    pub fn delete_entry_meta_and_data(
        &self,
        path: &str,
        is_recursive: bool,
        should_delete_chunks: bool,
    ) -> Result<(), FilerError> {
        if let Some(entry) = self.find_entry(path)? {
            if entry.is_directory() {
                let mut limit = 1;
                if is_recursive {
                    limit = i32::MAX;
                }
                let mut last_filename = String::new();
                let include_last_file = false;

                while limit > 0 {
                    let entries =
                        self.list_directory_entries(path, &last_filename, include_last_file, 1024)?;
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
                            )?;
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

            return self.filer_store()?.delete_entry(path);
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
    TrySend(#[from] TrySendError<Option<FileId>>),

    #[error("Io error: {0}")]
    Io(#[from] std::io::Error),
}
