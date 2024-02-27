use std::{
    fs::Metadata,
    io::Result,
    ops::{Deref, DerefMut},
    os::unix::fs::OpenOptionsExt,
    path::Path,
};

pub struct File {
    file: monoio::fs::File,

    name: std::path::PathBuf,
}

impl File {
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        OpenOptions::new().read(true).open(path.as_ref()).await
    }

    pub fn to_std(&self) -> Result<std::fs::File> {
        use std::os::fd::{AsRawFd, FromRawFd};

        let file = unsafe { std::fs::File::from_raw_fd(self.file.as_raw_fd()) };
        Ok(file)
    }

    pub fn from_std<P: AsRef<Path>>(file: std::fs::File, path: P) -> Result<Self> {
        let file = monoio::fs::File::from_std(file)?;
        Ok(Self {
            file,
            name: path.as_ref().to_path_buf(),
        })
    }

    pub fn metadata(&self) -> Result<Metadata> {
        std::fs::metadata(&self.name)
    }
}

impl Deref for File {
    type Target = monoio::fs::File;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

impl DerefMut for File {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.file
    }
}

#[derive(Clone, Debug)]
pub struct OpenOptions(monoio::fs::OpenOptions);

impl OpenOptions {
    #[must_use]
    pub fn new() -> Self {
        Self(monoio::fs::OpenOptions::new())
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

    pub async fn open<P: AsRef<Path>>(&self, path: P) -> Result<File> {
        let file = self.0.open(path.as_ref()).await?;
        Ok(File {
            file,
            name: path.as_ref().to_path_buf(),
        })
    }
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

pub type BufResult<T, B> = (Result<T>, B);
