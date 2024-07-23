use clap::{Args, Parser, Subcommand};
use faststr::FastStr;

#[derive(Parser, Debug)]
#[command(name = "helyim")]
#[command(author, version, about, long_about = None)]
pub struct Opts {
    #[command(flatten)]
    pub log: LogOptions,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    Master(MasterOptions),
    Volume(VolumeOptions),
    Filer(FilerOptions),
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
    /// pulse in second
    #[arg(long, default_value_t = 5)]
    pub pulse: u64,
    #[arg(long, default_value_t = 30000)]
    pub volume_size_limit_mb: u64,
    /// default replication if not specified
    #[arg(long, default_value("000"))]
    pub default_replication: FastStr,
    #[command(flatten)]
    pub raft: RaftOptions,
}

impl MasterOptions {
    pub fn check_raft_peers(&mut self) {
        let this_node = FastStr::new(format!("{}:{}", self.ip, self.port));
        if !self.raft.peers.contains(&this_node) {
            self.raft.peers.push(this_node);
        }
    }
}

// TODO: if clap support prefix in flatten derive, the following fields will remove prefix `raft_`
#[derive(Args, Debug, Clone)]
pub struct RaftOptions {
    /// raft peer in cluster, if not present, treat it as leader
    #[arg(long)]
    pub peers: Vec<FastStr>,
}

#[derive(Args, Debug)]
pub struct VolumeOptions {
    #[arg(long, default_value("127.0.0.1"))]
    pub ip: FastStr,
    #[arg(long, default_value_t = 8080)]
    pub port: u16,
    /// pulse in second
    #[arg(long, default_value_t = 5)]
    pub pulse: u64,
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
    pub folders: Vec<FastStr>,
}

impl VolumeOptions {
    pub fn public_url(&self) -> FastStr {
        self.public_url
            .clone()
            .unwrap_or(format!("{}:{}", self.ip, self.port).into())
    }

    pub fn paths(&self) -> Vec<String> {
        self.folders
            .iter()
            .map(|x| match x.rfind(':') {
                Some(idx) => x[0..idx].to_string(),
                None => x.to_string(),
            })
            .collect()
    }

    pub fn max_volumes(&self) -> Vec<i64> {
        self.folders
            .iter()
            .map(|x| match x.rfind(':') {
                Some(idx) => x[idx + 1..].parse::<i64>().unwrap(),
                None => 7,
            })
            .collect()
    }
}

#[derive(Args, Debug, Clone)]
pub struct FilerOptions {
    #[arg(long, default_value("127.0.0.1"))]
    pub ip: FastStr,
    #[arg(long, default_value_t = 8888)]
    pub port: u16,
    #[arg(long, default_value("127.0.0.1"))]
    pub masters: Vec<FastStr>,
    // namespace, isolate different spaces
    #[arg(long, default_value("default"))]
    pub collection: FastStr,
    // default replication if not specified
    #[arg(long, default_value("000"))]
    pub default_replication: FastStr,
    // whether to redirect to volume server
    #[arg(long, default_value_t = true)]
    pub redirect_on_read: bool,
    // default data center
    #[arg(long, default_value(""))]
    pub data_center: FastStr,
    // default rack
    #[arg(long, default_value(""))]
    pub rack: FastStr,
    // whether to disable directory listing
    #[arg(long, default_value_t = false)]
    pub disable_dir_listing: bool,
    // split file lager than this size
    #[arg(long, default_value_t = 32)]
    pub max_mb: u32,
    // sub dir listing size
    #[arg(long, default_value_t = 100_000)]
    pub dir_listing_limit: u32,
}

#[derive(Args, Debug, Clone)]
pub struct LogOptions {
    #[arg(long, default_value("./target/logs"))]
    pub log_path: FastStr,
    #[arg(long, default_value("stdout"))]
    pub log_output: FastStr,
}
