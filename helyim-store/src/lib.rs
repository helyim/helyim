#![allow(dead_code)]

use helyim_common::sys::shutdown_signal;

mod args;
pub use args::VolumeOptions;

mod disk_location;
mod erasure_coding;
mod http;
mod needle;
pub use needle::NeedleMapType;
mod operation;
mod server;
mod store;
mod volume;

pub async fn start_volume(volume_opts: VolumeOptions) -> Result<(), Box<dyn std::error::Error>> {
    let mut server =
        server::VolumeServer::new(NeedleMapType::NeedleMapInMemory, volume_opts, false).await?;

    server.start().await?;
    shutdown_signal().await;
    server.stop().await?;

    Ok(())
}
