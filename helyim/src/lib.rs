#![allow(unused_attributes)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::module_inception)]
#![deny(unused_qualifications)]

pub mod client;
pub mod directory;

pub mod errors;
mod filer;
mod images;
mod operation;
mod proto;

pub mod raft;

pub mod storage;

mod sequence;
mod topology;
pub mod util;

const PHRASE: &str = "<h1>Hello, World!</h1>";
const DEFAULT: &str = "default";

#[cfg(not(all(target_os = "linux", feature = "iouring")))]
pub use monoio::spawn as rt_spawn;
#[cfg(all(target_os = "linux", feature = "iouring"))]
pub use tokio_uring::spawn as rt_spawn;
