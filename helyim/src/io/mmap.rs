use std::{
    fs::File,
    io::{Read, Result, Seek, SeekFrom, Write},
    os::unix::fs::FileExt,
};

use memmap2::MmapMut;

pub struct MmapFile {
    file: File,
    mmap: MmapMut,
    /// next append offset
    position: usize,
    offset: i64,
    /// append number
    appended: usize,
}

impl MmapFile {
    pub fn new(file: File, len: u64) -> Result<Self> {
        let position = file.metadata()?.len();
        file.set_len(len)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        Ok(Self {
            file,
            mmap,
            position: position as usize,
            offset: 0,
            appended: 0,
        })
    }

    pub fn flush_async(&mut self) -> Result<()> {
        self.mmap.flush_async()
    }

    pub fn len(&self) -> usize {
        self.position
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
        // (&mut self.mmap.[offset as usize..offset as usize+buf.len()]).write(buf)
        unimplemented!("MmapFile: write_at is not supported")
    }
}

impl Read for MmapFile {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        (&self.mmap[self.position - buf.len()..self.position]).read(buf)
    }
}

impl Write for MmapFile {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let write = (&mut self.mmap[self.position..self.position + buf.len()]).write(buf)?;
        self.position += write;
        Ok(write)
    }

    fn flush(&mut self) -> Result<()> {
        self.mmap.flush()
    }
}

impl Seek for MmapFile {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        os::unix::fs::FileExt,
    };

    use crate::io::mmap::MmapFile;

    #[test]
    pub fn test_mmap() {
        let file = tempfile::tempfile().unwrap();
        let mut mmap = MmapFile::new(file, 1200).unwrap();
        for i in 0..100 {
            mmap.write_all(b"hello world").unwrap();
            assert_eq!(mmap.len(), 11 * (i + 1));
        }

        let len = mmap.as_file().metadata().unwrap().len();
        assert_eq!(len as usize, mmap.mmap.len());
        assert_eq!(len, 1200);

        let mut buf = vec![0; 11];
        for _ in 0..100 {
            mmap.read_exact(&mut buf).unwrap();
            assert_eq!(buf, b"hello world");
        }

        for _ in 0..100 {
            mmap.read_exact_at(&mut buf, 10).unwrap();
            assert_eq!(buf, b"dhello worl");
        }
    }
}
