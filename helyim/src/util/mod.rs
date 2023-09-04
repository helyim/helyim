#[macro_use]
pub mod macros;

mod http;

use faststr::FastStr;
pub use http::{delete, get, post};

use crate::DEFAULT;

pub mod time;

pub fn get_or_default(s: String) -> FastStr {
    if s.is_empty() {
        FastStr::from_static_str(DEFAULT)
    } else {
        FastStr::new(s)
    }
}
