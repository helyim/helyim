use std::{fs::metadata, io::ErrorKind, os::unix::fs::MetadataExt, path::Path, time::SystemTime};

use faststr::FastStr;
use mime_guess::mime;

pub fn check_file(filename: &str) -> Result<Option<(bool, bool, SystemTime, u64)>, std::io::Error> {
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

pub fn file_exists(filename: &str) -> Result<bool, std::io::Error> {
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

pub fn file_name(path: &str) -> String {
    let path = Path::new(path);
    path.file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap()
}

pub fn dir_and_name(path: &str) -> (String, String) {
    let path = Path::new(path);
    let dir = path.parent().map_or("/".to_string(), |path| {
        path.as_os_str().to_string_lossy().to_string()
    });
    let file_name = path
        .file_name()
        .map_or("".to_string(), |name| name.to_string_lossy().to_string());

    (dir, file_name)
}

pub fn new_full_path(dir: &str, filename: &str) -> String {
    let path = Path::new(dir);
    path.join(filename)
        .as_os_str()
        .to_string_lossy()
        .to_string()
}

pub fn guess_mimetype(filename: &str) -> FastStr {
    let mut guess_mtype = String::new();
    if let Some(idx) = filename.find('.') {
        let ext = &filename[idx..];
        let m = mime_guess::from_ext(ext).first_or_octet_stream();
        if m.type_() != mime::APPLICATION || m.subtype() != mime::OCTET_STREAM {
            guess_mtype.push_str(m.type_().as_str());
            guess_mtype.push('/');
            guess_mtype.push_str(m.subtype().as_str());
        }
    }

    guess_mtype.into()
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::OsStr,
        fs::{self, metadata, read, write},
        io::{ErrorKind, Read, Write},
        path::Path,
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
}
