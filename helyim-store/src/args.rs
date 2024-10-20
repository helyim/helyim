use clap::Args;
use faststr::FastStr;

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
