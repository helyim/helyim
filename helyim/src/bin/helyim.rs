use std::{io::stdout, time::Duration};

use clap::Parser;
use helyim::{
    directory::{DirectoryServer, Sequencer, SequencerType},
    filer::FilerServer,
    storage::{NeedleMapType, VolumeServer},
    util::{
        args::{Command, FilerOptions, LogOptions, MasterOptions, Opts, VolumeOptions},
        sys::shutdown_signal,
    },
};
use tracing::{info, Level};
use tracing_subscriber::{
    fmt, fmt::writer::MakeWriterExt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter,
    Registry,
};

async fn start_master(master_opts: MasterOptions) -> Result<(), Box<dyn std::error::Error>> {
    let sequencer = Sequencer::new(SequencerType::Memory)?;
    let mut directory = DirectoryServer::new(master_opts, 0.3, sequencer).await?;

    directory.start().await?;
    shutdown_signal().await;
    directory.stop().await?;

    tokio::time::sleep(Duration::from_secs(10)).await;
    Ok(())
}

async fn start_volume(volume_opts: VolumeOptions) -> Result<(), Box<dyn std::error::Error>> {
    let mut server =
        VolumeServer::new(NeedleMapType::NeedleMapInMemory, volume_opts, false).await?;

    server.start().await?;
    shutdown_signal().await;
    server.stop().await?;

    tokio::time::sleep(Duration::from_secs(10)).await;
    Ok(())
}

async fn start_filer(filer_opts: FilerOptions) -> Result<(), Box<dyn std::error::Error>> {
    let mut server = FilerServer::new(filer_opts).await?;

    server.start().await?;
    shutdown_signal().await;
    server.stop().await?;

    tokio::time::sleep(Duration::from_secs(10)).await;
    Ok(())
}

fn log_init(
    level: Level,
    opts: &LogOptions,
    log_prefix: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let env_filter =
        EnvFilter::from_default_env().add_directive(format!("helyim={level}").parse()?);
    let layered = Registry::default().with(env_filter);

    let layer = fmt::layer()
        .with_target(true)
        .with_level(true)
        .with_ansi(true)
        .with_line_number(true);

    if opts.log_path.is_empty() {
        let console_layer = layer.with_writer(stdout.with_max_level(level));
        layered.with(console_layer).init();
    } else {
        let file_appender = tracing_appender::rolling::daily(
            opts.log_path.as_str(),
            format!("helyim-{}.log", log_prefix),
        );
        let file_layer = layer.with_writer(file_appender.with_max_level(level));
        layered.with(file_layer).init();
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let level = Level::INFO;

    let opts = Opts::parse();
    info!("opts: {:?}", opts);

    let log_opts = opts.log.clone();
    match opts.command {
        Command::Master(mut master) => {
            let log_prefix = &format!("master-{}-{}", master.ip, master.port);
            log_init(level, &log_opts, log_prefix)?;
            master.check_raft_peers();
            info!("starting master server....");
            start_master(master).await
        }
        Command::Volume(volume) => {
            let log_prefix = &format!("volume-{}-{}", volume.ip, volume.port);
            log_init(level, &log_opts, log_prefix)?;

            info!("starting volume....");
            start_volume(volume).await
        }
        Command::Filer(filer) => {
            let log_prefix = &format!("filer-{}-{}", filer.ip, filer.port);
            log_init(level, &log_opts, log_prefix)?;

            info!("starting filer....");
            start_filer(filer).await
        }
    }
}
