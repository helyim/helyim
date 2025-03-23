use std::{
    cmp::max,
    fs::metadata,
    io,
    io::ErrorKind,
    os::unix::fs::MetadataExt,
    path::{MAIN_SEPARATOR, Path},
    time::SystemTime,
};

pub fn check_file(filename: &str) -> Result<Option<(bool, bool, SystemTime, u64)>, io::Error> {
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
                Err(err)
            }
        }
    }
}

pub fn file_exists(filename: &str) -> Result<bool, io::Error> {
    match metadata(filename) {
        Ok(_) => Ok(true),
        Err(err) => {
            if err.kind() == ErrorKind::NotFound {
                Ok(false)
            } else {
                Err(err)
            }
        }
    }
}

pub fn file_name(path: &str) -> Option<String> {
    let path = Path::new(path);
    path.file_name()
        .map(|name| name.to_string_lossy().to_string())
}

pub fn join_path(paths: &[&str], index: usize) -> String {
    let mut path = String::new();
    path.push(MAIN_SEPARATOR);

    let index = max(1, index) - 1;

    for p in paths[..index].iter() {
        path.push_str(p);
        path.push(MAIN_SEPARATOR);
    }

    if path.len() > 1 {
        path.remove(path.len() - 1);
    }
    path
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::OsStr,
        fs::{self, metadata, read, write},
        io::{ErrorKind, Read, Write},
        path::Path,
    };

    use crate::file::join_path;

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
            .truncate(true)
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
            .truncate(true)
            .read(true)
            .open(path)
            .unwrap();
        let mut buf = [0u8; 8];
        let n = file.read(&mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    pub fn test_file_ext() {
        let path = Path::new("/tmp/helyim.txt");
        assert_eq!(path.extension(), Some(OsStr::new("txt")));
    }

    #[test]
    pub fn test_join_path() {
        let paths = vec!["home", "admin", "data", "db"];
        assert_eq!(join_path(&paths, 0), "/".to_string());
        assert_eq!(join_path(&paths, 1), "/".to_string());
        assert_eq!(join_path(&paths, 2), "/home".to_string());
        assert_eq!(join_path(&paths, 3), "/home/admin".to_string());
        assert_eq!(join_path(&paths, 4), "/home/admin/data".to_string());
    }
}
