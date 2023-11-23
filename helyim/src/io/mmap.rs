use std::{
    fs::File,
    io::{Read, Result, Write},
};

use memmap2::MmapMut;

pub struct MmapFile {
    mmap: MmapMut,
    /// next append offset
    position: usize,
    /// append number
    appended: usize,
}

impl MmapFile {
    pub fn new(file: &File, len: u64) -> Result<Self> {
        let position = file.metadata()?.len();
        file.set_len(len)?;
        let mmap = unsafe { MmapMut::map_mut(file)? };
        Ok(Self {
            mmap,
            position: position as usize,
            appended: 0,
        })
    }

    pub fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        (&self.mmap[(self.position - buf.len())..self.position]).read_exact(buf)
    }

    pub fn read_exact_at(&mut self, buf: &mut [u8], offset: usize) -> Result<()> {
        (&self.mmap[offset..offset + buf.len()]).read_exact(buf)
    }

    pub fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        let write_all = (&mut self.mmap[self.position..]).write_all(buf);
        self.position += buf.len();
        self.appended += 1;
        if self.appended % 3 == 0 {
            self.flush_async()?;
        }
        write_all
    }

    pub fn flush(&mut self) -> Result<()> {
        self.mmap.flush()
    }

    pub fn flush_async(&mut self) -> Result<()> {
        self.mmap.flush_async()
    }
}

#[cfg(test)]
mod tests {
    use crate::io::mmap::MmapFile;

    #[test]
    pub fn test_mmap() {
        let file = tempfile::tempfile().unwrap();
        let mut mmap = MmapFile::new(&file, 1100).unwrap();
        for _ in 0..100 {
            mmap.write_all(b"hello world").unwrap();
        }

        assert_eq!(file.metadata().unwrap().len(), 1100);

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
