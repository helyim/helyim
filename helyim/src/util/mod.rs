#![allow(dead_code)]
use faststr::FastStr;

pub mod args;

pub mod file;

pub mod grpc;

pub mod http;

#[macro_use]
pub mod macros;

pub mod parser;

pub mod sys;

pub mod time;

pub fn get_or_default(s: String) -> FastStr {
    if s.is_empty() {
        FastStr::from_static_str(crate::DEFAULT)
    } else {
        FastStr::new(s)
    }
}
