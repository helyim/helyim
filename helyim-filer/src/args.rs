use clap::Args;
use faststr::FastStr;

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
    pub max_mb: i32,
    // sub dir listing size
    #[arg(long, default_value_t = 100_000)]
    pub dir_listing_limit: u32,
}
