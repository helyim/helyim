#![allow(dead_code)]

use helyim_common::sys::shutdown_signal;

mod args;
pub use args::FilerOptions;
mod deletion;
mod entry;
mod file_chunk;
mod http;
mod operation;
mod server;

mod filer;

mod store;

pub async fn start_filer(filer_opts: FilerOptions) -> Result<(), Box<dyn std::error::Error>> {
    let mut server = server::FilerServer::new(filer_opts).await?;

    server.start().await?;
    shutdown_signal().await;
    server.stop().await?;

    Ok(())
}
