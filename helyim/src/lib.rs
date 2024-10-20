use clap::{Args, Parser, Subcommand};
use faststr::FastStr;
use helyim_directory::MasterOptions;
use helyim_filer::FilerOptions;
use helyim_store::args::VolumeOptions;

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
pub struct LogOptions {
    #[arg(long, default_value(""))]
    pub log_path: FastStr,
}
