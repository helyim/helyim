use std::{fs, io::Write, os::unix::fs::OpenOptionsExt};

use crate::{errors::Result, storage::MemoryNeedleValueMap};

/// generates .ecx file from existing .idx file all keys are sorted in ascending order
fn write_sorted_file_from_idx(base_filename: &str, ext: &str) -> Result<()> {
    let mut nm = MemoryNeedleValueMap::new();
    nm.load_from_index(&format!("{}.idx", base_filename))?;
    let mut ecx_file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o644)
        .open(format!("{}{}", base_filename, ext))?;
    for (key, value) in nm.iter() {
        let buf = value.as_bytes(*key);
        ecx_file.write_all(&buf)?;
    }
    Ok(())
}
