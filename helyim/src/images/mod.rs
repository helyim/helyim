use std::{cell::OnceCell, path::Path};

use bytes::Bytes;
use tracing::error;

use crate::errors::Result;

pub struct Favicon {
    path: &'static str,
    cell: OnceCell<Bytes>,
}

unsafe impl Sync for Favicon {}

impl Favicon {
    pub const fn new(path: &'static str) -> Self {
        Self {
            path,
            cell: OnceCell::new(),
        }
    }
    pub fn bytes(&self) -> Result<&[u8]> {
        let buf = self.cell.get_or_init(|| {
            let path = Path::new("src/images").join(self.path);
            match std::fs::read(&path) {
                Ok(image) => Bytes::from(image),
                Err(err) => {
                    error!("loading favicon.ico failed. {err}");
                    Bytes::new()
                }
            }
        });
        Ok(buf)
    }
}

pub static FAVICON_ICO: Favicon = Favicon::new("favicon/favicon.ico");
