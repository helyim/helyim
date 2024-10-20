use helyim_common::sys::shutdown_signal;

mod deletion;
mod entry;
mod file_chunk;
mod http;
mod operation;
mod server;
pub use server::{start_filer_server, FilerServer};

mod filer;

mod args;
pub use args::FilerOptions;

pub async fn start_filer(filer_opts: FilerOptions) -> Result<(), Box<dyn std::error::Error>> {
    let mut server = FilerServer::new(filer_opts).await?;

    server.start().await?;
    shutdown_signal().await;
    server.stop().await?;

    Ok(())
}
