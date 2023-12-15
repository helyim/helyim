use clap::{Args, Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "helyim")]
#[command(author, version, about, long_about = None)]
pub struct Opts {
    #[arg(long, default_value("0.0.0.0"))]
    pub host: String,
    #[arg(long, default_value("./target/logs"))]
    pub log_path: String,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    Master(MasterOptions),
    Volume(VolumeOptions),
}

#[derive(Args, Debug)]
#[command(args_conflicts_with_subcommands = true)]
pub struct MasterOptions {
    #[arg(long, default_value("127.0.0.1"))]
    pub ip: String,
    #[arg(long, default_value_t = 9333)]
    pub port: u16,
    #[arg(long, default_value("./"))]
    pub meta_path: String,
    #[arg(long, default_value_t = 5)]
    pub pulse_seconds: u64,
    #[arg(long, default_value_t = 30000)]
    pub volume_size_limit_mb: u64,
    /// default replication if not specified
    #[arg(long, default_value("000"))]
    pub default_replication: String,
    #[command(flatten)]
    pub raft: RaftOptions,
}

// TODO: if clap support prefix in flatten derive, the following fields will remove prefix `raft_`
#[derive(Args, Debug)]
pub struct RaftOptions {
    #[arg(long, default_value("1:127.0.0.1:8333"))]
    pub raft_node: String,
    #[arg(long)]
    pub raft_leader: Option<String>,
}

#[derive(Args, Debug)]
pub struct VolumeOptions {
    #[arg(long, default_value("127.0.0.1"))]
    pub ip: String,
    #[arg(long, default_value_t = 8080)]
    pub port: u16,
    #[arg(long, default_value_t = 5)]
    pub pulse_seconds: i64,
    /// public access url
    #[arg(long)]
    pub public_url: Option<String>,
    /// default replication if not specified
    #[arg(long, default_value("000"))]
    pub default_replication: String,
    /// data center
    #[arg(long, default_value(""))]
    pub data_center: String,
    /// rack
    #[arg(long, default_value(""))]
    pub rack: String,
    /// master server endpoint
    #[arg(long, default_value("127.0.0.1:9333"))]
    pub master_server: String,
    /// directories to store data files
    #[arg(long)]
    pub dir: Vec<String>,
}
