use std::time::Duration;

use faststr::FastStr;
use helyim_common::{
    args::MasterOptions,
    sequence::{Sequencer, SequencerType},
    sys::shutdown_signal,
};

use crate::server::DirectoryServer;

mod errors;
pub mod http;
pub mod operation;
pub mod server;

const DEFAULT: &str = "default";
pub fn get_or_default(s: &str) -> FastStr {
    if s.is_empty() {
        FastStr::from_static_str(DEFAULT)
    } else {
        FastStr::new(s)
    }
}

pub async fn start_master(master_opts: MasterOptions) -> Result<(), Box<dyn std::error::Error>> {
    let sequencer = Sequencer::new(SequencerType::Memory)?;
    let mut directory = DirectoryServer::new(master_opts, 0.3, sequencer).await?;

    directory.start().await?;
    shutdown_signal().await;
    directory.stop().await?;

    tokio::time::sleep(Duration::from_secs(10)).await;
    Ok(())
}
