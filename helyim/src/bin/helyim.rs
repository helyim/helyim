use clap::{Args, Parser, Subcommand};
use helyim::{
    directory::{DirectoryServer, Sequencer, SequencerType},
    storage::{NeedleMapType, StorageServer},
};
use tokio::signal;
use tracing::{info, Level};

#[derive(Parser, Debug)]
#[command(name = "helyim")]
#[command(author, version, about, long_about = None)]
struct Opts {
    #[arg(long, default_value("0.0.0.0"))]
    host: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Master(MasterOptions),
    Volume(VolumeOptions),
}

#[derive(Args, Debug)]
struct MasterOptions {
    #[arg(long, default_value("127.0.0.1"))]
    ip: String,
    #[arg(long, default_value_t = 9333)]
    port: u16,
    #[arg(long, default_value("./"))]
    meta_dir: String,
    #[arg(long, default_value_t = 5)]
    pulse_seconds: u64,
    #[arg(long, default_value_t = 30000)]
    volume_size_limit_mb: u64,
    /// default replication if not specified
    #[arg(long, default_value("000"))]
    default_replication: String,
}

#[derive(Args, Debug)]
struct VolumeOptions {
    #[arg(long, default_value("127.0.0.1"))]
    ip: String,
    #[arg(long, default_value_t = 8080)]
    port: u16,
    #[arg(long, default_value_t = 5)]
    pulse_seconds: i64,
    /// public access url
    #[arg(long)]
    public_url: Option<String>,
    /// default replication if not specified
    #[arg(long, default_value("000"))]
    default_replication: String,
    /// data center
    #[arg(long, default_value(""))]
    data_center: String,
    /// rack
    #[arg(long, default_value(""))]
    rack: String,
    /// master server endpoint
    #[arg(long, default_value("127.0.0.1:9333"))]
    master_server: String,
    /// directories to store data files
    #[arg(long)]
    dir: Vec<String>,
}

async fn start_master(host: &str, master: MasterOptions) -> Result<(), Box<dyn std::error::Error>> {
    let mut dir = DirectoryServer::new(
        host,
        &master.ip,
        master.port,
        &master.meta_dir,
        master.volume_size_limit_mb,
        master.pulse_seconds,
        &master.default_replication,
        0.3,
        Sequencer::new(SequencerType::Memory)?,
    )
    .await?;
    dir.start().await?;
    shutdown_signal().await;
    dir.stop().await?;
    Ok(())
}

async fn start_volume(host: &str, volume: VolumeOptions) -> Result<(), Box<dyn std::error::Error>> {
    let public_url = volume
        .public_url
        .unwrap_or(format!("{}:{}", volume.ip, volume.port));
    let paths: Vec<String> = volume
        .dir
        .iter()
        .map(|x| match x.rfind(':') {
            Some(idx) => x[0..idx].to_string(),
            None => x.to_string(),
        })
        .collect();

    let max_volumes = volume
        .dir
        .iter()
        .map(|x| match x.rfind(':') {
            Some(idx) => x[idx + 1..].parse::<i64>().unwrap(),
            None => 7,
        })
        .collect();

    let mut server = StorageServer::new(
        host,
        &volume.ip,
        volume.port,
        &public_url,
        paths,
        max_volumes,
        NeedleMapType::NeedleMapInMemory,
        &volume.master_server,
        volume.pulse_seconds,
        &volume.data_center,
        &volume.rack,
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

fn log_init(level: Level) {
    tracing_subscriber::fmt()
        .with_target(true)
        .with_level(true)
        .with_max_level(level)
        .with_ansi(true)
        .with_line_number(true)
        .init();
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
    log_init(Level::INFO);

    let opts = Opts::parse();
    info!("opts: {:?}", opts);

    match opts.command {
        Command::Master(master) => {
            info!("starting master server....");
            start_master(&opts.host, master).await
        }
        Command::Volume(volume) => {
            info!("starting volume....");
            start_volume(&opts.host, volume).await
        }
    }
}
