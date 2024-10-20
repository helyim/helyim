use helyim_common::sys::shutdown_signal;

use crate::{args::VolumeOptions, needle::NeedleMapType, server::VolumeServer};

pub mod args;
pub mod disk_location;
pub mod erasure_coding;
pub mod http;
pub mod needle;
pub mod operation;
pub mod server;
mod store;
pub mod volume;

pub async fn start_volume(volume_opts: VolumeOptions) -> Result<(), Box<dyn std::error::Error>> {
    let mut server =
        VolumeServer::new(NeedleMapType::NeedleMapInMemory, volume_opts, false).await?;

    server.start().await?;
    shutdown_signal().await;
    server.stop().await?;

    Ok(())
}
