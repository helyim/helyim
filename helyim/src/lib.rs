#![allow(clippy::too_many_arguments)]
use std::time::Duration;

pub mod directory;

mod errors;
mod images;
mod operation;

pub mod storage;

mod sequence;
mod util;

const PHRASE: &str = "<h1>Hello, World!</h1>";
const DEFAULT: &str = "default";
const STOP_INTERVAL: Duration = Duration::from_secs(2);

#[cfg(not(all(target_os = "linux", feature = "iouring")))]
pub use tokio::spawn as rt_spawn;
#[cfg(all(target_os = "linux", feature = "iouring"))]
pub use tokio_uring::spawn as rt_spawn;
