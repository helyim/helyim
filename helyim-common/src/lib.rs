pub mod connector;
pub mod consts;
pub mod crc;
pub mod file;
pub mod http;
pub mod images;

#[macro_use]
pub mod macros;
pub mod operation;

pub mod parser;

pub mod sequence;
pub mod sys;
pub mod time;
pub mod ttl;
pub mod types;
pub mod version;

pub fn grpc_port(port: u16) -> u16 {
    port + 10000
}
