#![allow(dead_code)]
use faststr::FastStr;

#[macro_use]
pub mod macros;

mod http;

pub use http::{delete, post, FormOrJson};

mod sys;
pub use sys::exit;

pub mod time;

pub fn get_or_default(s: String) -> FastStr {
    if s.is_empty() {
        FastStr::from_static_str(crate::DEFAULT)
    } else {
        FastStr::new(s)
    }
}
