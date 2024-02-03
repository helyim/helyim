use clap::Parser;
use helyim::{
    directory::{DirectoryServer, Sequencer, SequencerType},
    storage::{NeedleMapType, VolumeServer},
    util::{
        args::{Command, LogOptions, MasterOptions, Opts, VolumeOptions},
        sys::shutdown_signal,
    },
};
use tracing::{info, Level};
use tracing_subscriber::EnvFilter;

async fn start_master(master_opts: MasterOptions) -> Result<(), Box<dyn std::error::Error>> {
    let sequencer = Sequencer::new(SequencerType::Memory)?;
    let mut directory = DirectoryServer::new(master_opts, 0.3, sequencer).await?;

    directory.start().await?;
    shutdown_signal().await;
    directory.stop().await?;
    Ok(())
}

async fn start_volume(volume_opts: VolumeOptions) -> Result<(), Box<dyn std::error::Error>> {
    let mut server =
        VolumeServer::new(NeedleMapType::NeedleMapInMemory, volume_opts, false).await?;

    server.start().await?;
    shutdown_signal().await;
    server.stop().await?;

    Ok(())
}

fn log_init(
    level: Level,
    opts: &LogOptions,
    log_prefix: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("RUST_LOG", "none");
    let helyim = env!("CARGO_PKG_NAME");
    let filter = EnvFilter::from_default_env().add_directive(format!("{helyim}={level}").parse()?);
    let file_appender = tracing_appender::rolling::daily(
        opts.log_path.as_str(),
        format!("helyim-{}.log", log_prefix),
    );
    let subscriber = tracing_subscriber::fmt()
        .with_writer(file_appender)
        .with_env_filter(filter)
        .with_target(true)
        .with_level(true)
        .with_max_level(level)
        // .with_max_level(level)
        .with_ansi(true)
        .with_line_number(true)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}

#[tokio::main]
#[cfg(not(all(target_os = "linux", feature = "iouring")))]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    main_inner().await
}

#[cfg(all(target_os = "linux", feature = "iouring"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio_uring::start(main_inner())
}

async fn main_inner() -> Result<(), Box<dyn std::error::Error>> {
    let level = Level::INFO;

    let opts = Opts::parse();
    info!("opts: {:?}", opts);

    let log_opts = opts.log.clone();
    match opts.command {
        Command::Master(mut master) => {
            log_init(level, &log_opts, "master")?;

            master.check_raft_peers();

            info!("starting master server....");
            start_master(master).await
        }
        Command::Volume(volume) => {
            log_init(
                level,
                &log_opts,
                &format!("volume-{}-{}", volume.ip, volume.port),
            )?;

            info!("starting volume....");
            start_volume(volume).await
        }
    }
}
