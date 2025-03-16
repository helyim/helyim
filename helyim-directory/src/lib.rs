#![allow(dead_code)]

use faststr::FastStr;
use helyim_common::{
    sequence::{Sequencer, SequencerType},
    sys::shutdown_signal,
};

mod args;
mod errors;
mod http;
mod operation;
mod server;
pub use args::MasterOptions;

fn get_or_default(s: &str) -> FastStr {
    if s.is_empty() {
        FastStr::from_static_str("default")
    } else {
        FastStr::new(s)
    }
}

pub async fn start_master(master_opts: MasterOptions) -> Result<(), Box<dyn std::error::Error>> {
    let sequencer = Sequencer::new(SequencerType::Memory)?;
    let mut directory = server::DirectoryServer::new(master_opts, 0.3, sequencer).await?;

    directory.start().await?;
    shutdown_signal().await;
    directory.stop().await?;

    Ok(())
}
