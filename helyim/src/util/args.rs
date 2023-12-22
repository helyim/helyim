use clap::{Args, Parser, Subcommand};
use faststr::FastStr;

use crate::raft::types::NodeId;

#[derive(Parser, Debug)]
#[command(name = "helyim")]
#[command(author, version, about, long_about = None)]
pub struct Opts {
    #[arg(long, default_value("0.0.0.0"))]
    pub host: FastStr,
    #[command(flatten)]
    pub log: LogOptions,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    Master(MasterOptions),
    Volume(VolumeOptions),
}

#[derive(Args, Debug, Clone)]
#[command(args_conflicts_with_subcommands = true)]
pub struct MasterOptions {
    #[arg(long, default_value("127.0.0.1"))]
    pub ip: FastStr,
    #[arg(long, default_value_t = 9333)]
    pub port: u16,
    #[arg(long, default_value("./"))]
    pub meta_path: FastStr,
    #[arg(long, default_value_t = 5)]
    pub pulse_seconds: u64,
    #[arg(long, default_value_t = 30000)]
    pub volume_size_limit_mb: u64,
    /// default replication if not specified
    #[arg(long, default_value("000"))]
    pub default_replication: FastStr,
    #[command(flatten)]
    pub raft: RaftOptions,
}

// TODO: if clap support prefix in flatten derive, the following fields will remove prefix `raft_`
#[derive(Args, Debug, Clone)]
pub struct RaftOptions {
    #[arg(long, default_value_t = 1)]
    pub node_id: NodeId,
    #[arg(long)]
    pub peer: Option<FastStr>,
}

#[derive(Args, Debug)]
pub struct VolumeOptions {
    #[arg(long, default_value("127.0.0.1"))]
    pub ip: FastStr,
    #[arg(long, default_value_t = 8080)]
    pub port: u16,
    #[arg(long, default_value_t = 5)]
    pub pulse_seconds: u64,
    /// public access url
    #[arg(long)]
    pub public_url: Option<FastStr>,
    /// default replication if not specified
    #[arg(long, default_value("000"))]
    pub default_replication: FastStr,
    /// data center
    #[arg(long, default_value(""))]
    pub data_center: FastStr,
    /// rack
    #[arg(long, default_value(""))]
    pub rack: FastStr,
    /// master server endpoint
    #[arg(long, default_value("127.0.0.1:9333"))]
    pub master_server: FastStr,
    /// directories to store data files
    #[arg(long)]
    pub dir: Vec<FastStr>,
}

#[derive(Args, Debug, Clone)]
pub struct LogOptions {
    #[arg(long, default_value("./target/logs"))]
    pub log_path: FastStr,
    #[arg(long, default_value("stdout"))]
    pub log_output: FastStr,
}
