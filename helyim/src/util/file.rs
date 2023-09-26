use std::{fs::metadata, io::ErrorKind, os::unix::fs::MetadataExt, time::SystemTime};

use crate::errors::Result;

pub fn check_file(filename: &str) -> Result<Option<(bool, bool, SystemTime, u64)>> {
    match metadata(filename) {
        Ok(metadata) => {
            let mode = metadata.mode();
            let can_read = mode & 0o400 != 0;
            let can_write = mode & 0o200 != 0;
            let modified = metadata.modified()?;
            let filesize = metadata.len();
            Ok(Some((can_read, can_write, modified, filesize)))
        }
        Err(err) => {
            if err.kind() == ErrorKind::NotFound {
                Ok(None)
            } else {
                Err(err.into())
            }
        }
    }
}

pub fn file_exists(filename: &str) -> Result<bool> {
    match metadata(filename) {
        Ok(_) => Ok(true),
        Err(err) => {
            if err.kind() == ErrorKind::NotFound {
                Ok(false)
            } else {
                Err(err.into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        fs::{metadata, read, write},
        io::{ErrorKind, Read, Write},
    };

    #[test]
    pub fn test_file_exist() {
        let metadata = metadata("/tmp/not_exist_path");
        assert!(metadata.is_err_and(|err| err.kind() == ErrorKind::NotFound));
    }

    #[test]
    pub fn test_file_override() {
        let path = "/tmp/tmp_file_override";
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .open(path)
            .unwrap();
        file.write_all(b"hello world").unwrap();
        write(path, b"hello helyim").unwrap();
        assert_eq!(read(path).unwrap(), b"hello helyim");
    }

    #[test]
    pub fn test_eof() {
        let path = "/tmp/tmp_file_eof";
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .open(path)
            .unwrap();
        let mut buf = [0u8; 8];
        let n = file.read(&mut buf).unwrap();
        assert_eq!(n, 0);
    }
}
