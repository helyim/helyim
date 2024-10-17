use faststr::FastStr;

pub mod args;

pub mod chan;

pub mod file;

pub mod grpc;

pub mod http;

pub mod parser;

pub fn get_or_default(s: &str) -> FastStr {
    if s.is_empty() {
        FastStr::from_static_str(crate::DEFAULT)
    } else {
        FastStr::new(s)
    }
}
