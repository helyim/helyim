use std::{fs::File, os::unix::fs::FileExt};

use bytes::Bytes;

use crate::storage::{
    types::{Offset, Size},
    version::Version,
    Needle, NeedleError,
};

pub fn read_needle_blob_sync(
    file: &File,
    offset: Offset,
    size: Size,
) -> Result<Bytes, NeedleError> {
    let size = size.actual_size();
    let offset = offset.actual_offset();

    let mut buf = vec![0; size as usize];
    file.read_exact_at(&mut buf, offset)?;
    Ok(Bytes::from(buf))
}

impl Needle {
    pub fn read_data_sync(
        &mut self,
        file: &File,
        offset: Offset,
        size: Size,
        version: Version,
    ) -> Result<(), NeedleError> {
        let bytes = read_needle_blob_sync(file, offset, size)?;
        self.read_bytes(bytes, offset, size, version)
    }
}
