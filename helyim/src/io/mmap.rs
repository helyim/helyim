use std::{
    fs::File,
    io::{Error, ErrorKind, Read, Result, Seek, SeekFrom, Write},
    os::unix::fs::FileExt,
};

use memmap2::MmapMut;

pub struct MmapFile {
    file: File,
    mmap: MmapMut,
    /// real file length
    len: u64,
    pos: u64,
    preallocate: u64,
}

impl MmapFile {
    pub fn new(file: File, preallocate: u64) -> Result<Self> {
        let position = file.metadata()?.len();
        if preallocate > position {
            file.set_len(preallocate)?;
        }
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        Ok(Self {
            file,
            mmap,
            len: position,
            pos: position,
            preallocate,
        })
    }

    pub fn flush_async(&mut self) -> Result<()> {
        self.mmap.flush_async()
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn as_file(&self) -> &File {
        &self.file
    }

    pub fn as_file_mut(&mut self) -> &mut File {
        &mut self.file
    }
}

impl FileExt for MmapFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        (&self.mmap[offset as usize..offset as usize + buf.len()]).read(buf)
    }

    fn write_at(&self, _buf: &[u8], _offset: u64) -> Result<usize> {
        unimplemented!("MmapFile: write_at is not supported")
    }
}

impl Read for MmapFile {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let len = self.pos.min(self.len);
        let mut remaining = &self.mmap[(len as usize)..self.len as usize];
        let n = Read::read(&mut remaining, buf)?;
        self.pos += n as u64;
        Ok(n)
    }
}

impl Write for MmapFile {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let len = std::cmp::min(self.pos + buf.len() as u64, self.preallocate);
        let write = (&mut self.mmap[self.pos as usize..len as usize]).write(buf)?;
        self.len += write as u64;
        self.pos += write as u64;
        Ok(write)
    }

    fn flush(&mut self) -> Result<()> {
        self.mmap.flush()
    }
}

impl Seek for MmapFile {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        let (base_pos, offset) = match pos {
            SeekFrom::Start(n) => {
                self.pos = n;
                return Ok(n);
            }
            SeekFrom::End(n) => (self.len, n),
            SeekFrom::Current(n) => (self.pos, n),
        };
        let new_pos = if offset >= 0 {
            base_pos.checked_add(offset as u64)
        } else {
            base_pos.checked_sub((offset.wrapping_neg()) as u64)
        };
        match new_pos {
            Some(n) => {
                self.pos = n;
                Ok(self.pos)
            }
            None => Err(Error::new(
                ErrorKind::InvalidInput,
                "invalid seek to a negative or overflowing position",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Seek, SeekFrom, Write},
        os::unix::fs::FileExt,
    };

    use crate::io::mmap::MmapFile;

    #[test]
    pub fn test_mmap() {
        let file = tempfile::tempfile().unwrap();
        let mut mmap = MmapFile::new(file, 1100).unwrap();

        // 1. write to mmap
        mmap.seek(SeekFrom::Start(0)).unwrap();
        assert_eq!(mmap.pos, 0);
        for i in 0..100 {
            mmap.write_all(b"hello world").unwrap();
            assert_eq!(mmap.len(), 11 * (i + 1));
        }
        assert_eq!(mmap.pos, 1100);

        // 2. validate file size
        let len = mmap.as_file().metadata().unwrap().len();
        assert_eq!(len as usize, mmap.mmap.len());
        assert_eq!(len, 1100);

        // 3. read from mmap
        mmap.seek(SeekFrom::Start(0)).unwrap();
        let mut buf = vec![0; 11];
        for _ in 0..100 {
            mmap.read_exact(&mut buf).unwrap();
            assert_eq!(buf, b"hello world");
        }
        assert_eq!(mmap.pos, 1100);

        // 4. randomly read from mmap, will not change mmap.pos
        for _ in 0..100 {
            mmap.read_exact_at(&mut buf, 0).unwrap();
            assert_eq!(buf, b"hello world");

            mmap.read_exact_at(&mut buf, 10).unwrap();
            assert_eq!(buf, b"dhello worl");

            mmap.read_exact_at(&mut buf, 20).unwrap();
            assert_eq!(buf, b"ldhello wor");

            mmap.read_exact_at(&mut buf, 30).unwrap();
            assert_eq!(buf, b"rldhello wo");

            mmap.read_exact_at(&mut buf, 40).unwrap();
            assert_eq!(buf, b"orldhello w");

            mmap.read_exact_at(&mut buf, 50).unwrap();
            assert_eq!(buf, b"worldhello ");

            mmap.read_exact_at(&mut buf, 60).unwrap();
            assert_eq!(buf, b" worldhello");

            mmap.read_exact_at(&mut buf, 70).unwrap();
            assert_eq!(buf, b"o worldhell");

            mmap.read_exact_at(&mut buf, 80).unwrap();
            assert_eq!(buf, b"lo worldhel");

            mmap.read_exact_at(&mut buf, 90).unwrap();
            assert_eq!(buf, b"llo worldhe");

            mmap.read_exact_at(&mut buf, 100).unwrap();
            assert_eq!(buf, b"ello worldh");
        }
        assert_eq!(mmap.pos, 1100);

        mmap.seek(SeekFrom::Start(0)).unwrap();
        mmap.read(&mut buf).unwrap();
        assert_eq!(buf, b"hello world");
        assert_eq!(mmap.pos, 11);

        mmap.seek(SeekFrom::Start(22)).unwrap();
        mmap.read(&mut buf).unwrap();
        assert_eq!(buf, b"hello world");
        assert_eq!(mmap.pos, 33);

        mmap.seek(SeekFrom::Current(2)).unwrap();
        assert_eq!(mmap.pos, 35);
        mmap.read(&mut buf).unwrap();
        assert_eq!(buf, b"llo worldhe");
        assert_eq!(mmap.pos, 46);

        mmap.seek(SeekFrom::Current(-2)).unwrap();
        assert_eq!(mmap.pos, 44);
        mmap.read(&mut buf).unwrap();
        assert_eq!(buf, b"hello world");
        assert_eq!(mmap.pos, 55);

        mmap.seek(SeekFrom::End(0)).unwrap();
        assert_eq!(mmap.pos, 1100);
        buf.clear();
        mmap.read(&mut buf).unwrap();
        assert_eq!(buf, vec![0u8; 0]);
        assert_eq!(mmap.pos, 1100);

        mmap.seek(SeekFrom::End(-1)).unwrap();
        assert_eq!(mmap.pos, 1099);
        assert!(mmap.write_all(b"hello world").is_err());
        assert_eq!(mmap.pos, 1100);
    }
}
