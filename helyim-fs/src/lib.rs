#[cfg(not(all(target_os = "linux", feature = "iouring")))]
use std::os::unix::fs::FileExt;
use std::{fs::Metadata, io::Result, os::unix::fs::OpenOptionsExt, path::Path};

#[cfg(all(target_os = "linux", feature = "iouring"))]
use tokio_uring::buf::IoBuf;

pub struct File {
    #[cfg(not(all(target_os = "linux", feature = "iouring")))]
    inner: std::fs::File,

    #[cfg(all(target_os = "linux", feature = "iouring"))]
    inner: tokio_uring::fs::File,

    #[cfg(all(target_os = "linux", feature = "iouring"))]
    name: std::path::PathBuf,
}

impl File {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        OpenOptions::new().read(true).open(path.as_ref())
    }

    pub fn to_std(&self) -> Result<std::fs::File> {
        cfg_if::cfg_if! {
            if #[cfg(all(target_os = "linux", feature = "iouring"))] {
                use std::os::fd::FromRawFd;
                use std::os::fd::AsRawFd;

                let file = unsafe { std::fs::File::from_raw_fd(self.inner.as_raw_fd()) };
                Ok(file)
            } else {
                self.inner.try_clone()
            }
        }
    }

    #[allow(unused_mut)]
    pub async fn read_at(&self, mut buf: Vec<u8>, offset: u64) -> BufResult<usize, Vec<u8>> {
        cfg_if::cfg_if! {
            if #[cfg(all(target_os = "linux", feature = "iouring"))] {
                self.inner.read_at(buf, offset).await
            } else {
                let read_at = self.inner.read_at(&mut buf, offset);
                (read_at, buf)
            }
        }
    }

    /// Read the exact number of bytes required to fill `buf` at the specified
    /// offset from the file.
    ///
    /// This function reads as many as bytes as necessary to completely fill the
    /// specified buffer `buf`.
    ///
    /// # Return
    ///
    /// The method returns the operation result and the same buffer value passed
    /// as an argument.
    ///
    /// If the method returns [`Ok(())`], then the read was successful.
    ///
    /// # Errors
    ///
    /// If this function encounters an error of the kind
    /// [`ErrorKind::Interrupted`] then the error is ignored and the
    /// operation will continue.
    ///
    /// If this function encounters an "end of file" before completely filling
    /// the buffer, it returns an error of the kind
    /// [`ErrorKind::UnexpectedEof`]. The buffer is returned on error.
    ///
    /// If this function encounters any form of I/O or other error, an error
    /// variant will be returned. The buffer is returned on error.
    ///
    /// refer to https://github.com/bytedance/monoio/blob/4c3fed563600b4b9de8f9526f1ddd897a797e31f/monoio/src/fs/file.rs#L194-L274
    pub async fn read_exact_at(&self, mut buf: Vec<u8>, offset: u64) -> BufResult<(), Vec<u8>> {
        cfg_if::cfg_if! {
            if #[cfg(all(target_os = "linux", feature = "iouring"))] {
                let len = buf.bytes_total();
                let mut read = 0;
                while read < len {
                    let slice = buf.slice(read..len);
                    let (res, slice) = self.inner.read_at(slice, offset + read as u64).await;
                    buf = slice.into_inner();
                    match res {
                        Ok(0) => {
                            return (
                                Err(std::io::Error::new(
                                    std::io::ErrorKind::UnexpectedEof,
                                    "failed to fill whole buffer",
                                )),
                                buf,
                            )
                        }
                        Ok(n) => {
                            read += n;
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                        Err(e) => return (Err(e), buf),
                    };
                }
                (Ok(()), buf)
            } else {
                let read_exact_at = self.inner.read_exact_at(&mut buf, offset);
                (read_exact_at, buf)
            }
        }
    }

    pub async fn write_at(&self, buf: Vec<u8>, offset: u64) -> BufResult<usize, Vec<u8>> {
        cfg_if::cfg_if! {
            if #[cfg(all(target_os = "linux", feature = "iouring"))] {
                self.inner.write_at(buf, offset).await
            } else {
                let write_at = self.inner.write_at(&buf, offset);
                (write_at, buf)
            }
        }
    }

    /// Attempts to write an entire buffer into this file at the specified
    /// offset.
    ///
    /// This method will continuously call [`write_at`] until there is no more
    /// data to be written or an error of non-[`ErrorKind::Interrupted`]
    /// kind is returned. This method will not return until the entire
    /// buffer has been successfully written or such an error occurs.
    ///
    /// If the buffer contains no data, this will never call [`write_at`].
    ///
    /// # Return
    ///
    /// The method returns the operation result and the same buffer value passed
    /// in as an argument.
    ///
    /// # Errors
    ///
    /// This function will return the first error of
    /// non-[`ErrorKind::Interrupted`] kind that [`write_at`] returns.
    ///
    /// refer to https://github.com/bytedance/monoio/blob/4c3fed563600b4b9de8f9526f1ddd897a797e31f/monoio/src/fs/file.rs#L326-L393
    #[allow(unused_mut)]
    pub async fn write_all_at(&self, mut buf: Vec<u8>, offset: u64) -> BufResult<(), Vec<u8>> {
        cfg_if::cfg_if! {
            if #[cfg(all(target_os = "linux", feature = "iouring"))] {
                let len = buf.bytes_init();
                let mut written = 0;
                while written < len {
                    let slice = buf.slice(written..len);
                    let (res, slice) = self.inner.write_at(slice, offset + written as u64).await;
                    buf = slice.into_inner();
                    match res {
                        Ok(0) => {
                            return (
                                Err(std::io::Error::new(
                                    std::io::ErrorKind::WriteZero,
                                    "failed to write whole buffer",
                                )),
                                buf,
                            )
                        }
                        Ok(n) => written += n,
                        Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                        Err(e) => return (Err(e), buf),
                    };
                }

                (Ok(()), buf)
            } else {
                let write_all_at = self.inner.write_all_at(&buf, offset);
                (write_all_at, buf)
            }
        }
    }

    pub fn metadata(&self) -> Result<Metadata> {
        cfg_if::cfg_if! {
            if #[cfg(all(target_os = "linux", feature = "iouring"))] {
                self.name.metadata()
            } else {
                self.inner.metadata()
            }
        }
    }

    pub async fn close(self) -> Result<()> {
        cfg_if::cfg_if! {
            if #[cfg(all(target_os = "linux", feature = "iouring"))] {
                self.inner.close().await
            } else {
                Ok(())
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct OpenOptions(std::fs::OpenOptions);

impl OpenOptions {
    #[must_use]
    pub fn new() -> Self {
        Self(std::fs::OpenOptions::new())
    }

    pub fn read(&mut self, read: bool) -> &mut Self {
        self.0.read(read);
        self
    }

    pub fn write(&mut self, write: bool) -> &mut Self {
        self.0.write(write);
        self
    }

    pub fn append(&mut self, append: bool) -> &mut Self {
        self.0.append(append);
        self
    }

    pub fn truncate(&mut self, truncate: bool) -> &mut Self {
        self.0.truncate(truncate);
        self
    }

    pub fn create(&mut self, create: bool) -> &mut Self {
        self.0.create(create);
        self
    }

    pub fn create_new(&mut self, create_new: bool) -> &mut Self {
        self.0.create_new(create_new);
        self
    }

    pub fn mode(&mut self, mode: u32) -> &mut Self {
        self.0.mode(mode);
        self
    }

    pub fn open<P: AsRef<Path>>(&self, path: P) -> Result<File> {
        let file = self.0.open(path.as_ref())?;
        cfg_if::cfg_if! {
            if #[cfg(all(target_os = "linux", feature = "iouring"))] {
                Ok(File {
                    inner: tokio_uring::fs::File::from_std(file),
                    name: path.as_ref().to_path_buf()
                })
            } else {
                Ok(File {
                    inner: file,
                })
            }
        }
    }
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

pub type BufResult<T, B> = (Result<T>, B);

#[cfg(test)]
mod tests {
    use std::io::Write;

    use crate::{File, OpenOptions};

    #[tokio::test]
    #[cfg(not(all(target_os = "linux", feature = "iouring")))]
    pub async fn test_read_exact_at() {
        let mut std_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open("/tmp/test_read_exact_at.txt")
            .unwrap();
        std_file.write_all(b"hello worldhello world").unwrap();

        let file = File::open("/tmp/test_read_exact_at.txt").unwrap();
        let buf = vec![0; 11];
        let (_, buf) = file.read_exact_at(buf, 0).await;

        assert_eq!(&buf[..], b"hello world");
        file.close().await.unwrap();
    }

    #[test]
    #[cfg(all(target_os = "linux", feature = "iouring"))]
    pub fn test_read_exact_at_uring() {
        let mut std_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open("/tmp/test_read_exact_at_uring.txt")
            .unwrap();
        std_file.write_all(b"hello worldhello world").unwrap();

        let file = File::open("/tmp/test_read_exact_at_uring.txt").unwrap();

        tokio_uring::start(async {
            let buf = vec![0; 11];
            let (_, buf) = file.read_exact_at(buf, 0).await;

            assert_eq!(&buf[..], b"hello world");
            file.close().await.unwrap();
        });
    }

    #[tokio::test]
    #[cfg(not(all(target_os = "linux", feature = "iouring")))]
    pub async fn test_write_exact_at() {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open("/tmp/test_write_exact_at.txt")
            .unwrap();

        let buf = b"hello world".to_vec();
        let (_, buf) = file.write_all_at(buf, 0).await;
        assert_eq!(&buf[..], b"hello world");

        let read_buf = vec![0; 11];
        let (ret, read_buf) = file.read_exact_at(read_buf, 0).await;

        assert!(ret.is_ok());
        assert_eq!(&read_buf[..], b"hello world");
        file.close().await.unwrap();
    }

    #[test]
    #[cfg(all(target_os = "linux", feature = "iouring"))]
    pub fn test_write_exact_at_uring() {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open("/tmp/test_write_exact_at.txt")
            .unwrap();

        tokio_uring::start(async {
            let buf = b"hello world".to_vec();
            let (_, buf) = file.write_all_at(buf, 0).await;
            assert_eq!(&buf[..], b"hello world");

            let read_buf = vec![0; 11];
            let (ret, read_buf) = file.read_exact_at(read_buf, 0).await;

            assert!(ret.is_ok());
            assert_eq!(&read_buf[..], b"hello world");

            file.close().await.unwrap();
        });
    }
}
