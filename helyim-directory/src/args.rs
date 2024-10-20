use clap::Args;
use faststr::FastStr;

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

#[derive(Args, Debug, Clone)]
pub struct RaftOptions {
    /// raft peer in cluster, if not present, treat it as leader
    #[arg(long)]
    pub peers: Vec<FastStr>,
}
