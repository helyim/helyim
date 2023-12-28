use clap::Parser;
use helyim::{
    directory::{DirectoryServer, Sequencer, SequencerType},
    storage::{NeedleMapType, VolumeServer},
    util::args::{Command, LogOptions, MasterOptions, Opts, VolumeOptions},
};
use tokio::signal;
use tracing::{info, Level};
use tracing_subscriber::EnvFilter;

async fn start_master(master_opts: MasterOptions) -> Result<(), Box<dyn std::error::Error>> {
    let mut dir =
        DirectoryServer::new(master_opts, 0.3, Sequencer::new(SequencerType::Memory)?).await?;
    dir.start().await?;
    shutdown_signal().await;
    dir.stop().await?;
    Ok(())
}

async fn start_volume(volume_opts: VolumeOptions) -> Result<(), Box<dyn std::error::Error>> {
    let public_url = volume_opts
        .public_url
        .clone()
        .unwrap_or(format!("{}:{}", volume_opts.ip, volume_opts.port).into());
    let paths: Vec<String> = volume_opts
        .dir
        .iter()
        .map(|x| match x.rfind(':') {
            Some(idx) => x[0..idx].to_string(),
            None => x.to_string(),
        })
        .collect();

    let max_volumes = volume_opts
        .dir
        .iter()
        .map(|x| match x.rfind(':') {
            Some(idx) => x[idx + 1..].parse::<i64>().unwrap(),
            None => 7,
        })
        .collect();

    let mut server = VolumeServer::new(
        &public_url,
        paths,
        max_volumes,
        NeedleMapType::NeedleMapInMemory,
        volume_opts,
        false,
    )
    .await?;
    server.start().await?;
    shutdown_signal().await;
    server.stop().await?;

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
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
        Command::Master(master) => {
            log_init(level, &log_opts, "master")?;

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
