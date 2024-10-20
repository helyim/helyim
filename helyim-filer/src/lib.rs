use helyim_common::{args::FilerOptions, sys::shutdown_signal};

use crate::entry::Entry;

pub mod deletion;
pub mod entry;
mod file_chunk;
mod http;
mod operation;
mod server;
pub use server::{start_filer_server, FilerServer};

use crate::filer::FilerError;

mod filer;

pub async fn start_filer(filer_opts: FilerOptions) -> Result<(), Box<dyn std::error::Error>> {
    let mut server = FilerServer::new(filer_opts).await?;

    server.start().await?;
    shutdown_signal().await;
    server.stop().await?;

    Ok(())
}

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
