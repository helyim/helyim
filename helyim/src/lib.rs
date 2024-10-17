#![allow(dead_code)]
#![allow(unused_attributes)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::module_inception)]
#![deny(unused_qualifications)]

pub mod client;
pub mod directory;

pub mod errors;
pub mod filer;
mod operation;
mod proto;

pub mod raft;

pub mod storage;
mod topology;
pub mod util;

const PHRASE: &str = "<h1>Hello, World!</h1>";
const DEFAULT: &str = "default";
