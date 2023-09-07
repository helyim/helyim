use std::cell::OnceCell;

use bytes::Bytes;

use crate::errors::Result;

pub struct Favicon {
    cell: OnceCell<Bytes>,
}

unsafe impl Sync for Favicon {}

impl Favicon {
    pub const fn new() -> Self {
        Self {
            cell: OnceCell::new(),
        }
    }
    pub fn bytes(&self) -> Result<&[u8]> {
        let buf = self
            .cell
            .get_or_init(|| Bytes::from_static(include_bytes!("favicon/favicon.ico")));
        Ok(buf)
    }
}

pub static FAVICON_ICO: Favicon = Favicon::new();
