use std::{
    collections::VecDeque,
    io::{Result, SeekFrom},
    path::{Path, PathBuf},
};

use tokio::{
    fs::{read_dir, File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt},
};

pub async fn open(path: &str) -> Result<File> {
    OpenOptions::new().read(true).open(path).await
}

pub async fn append(path: &str) -> Result<File> {
    OpenOptions::new().read(true).append(true).open(path).await
}

pub async fn overwrite(path: &str) -> Result<File> {
    OpenOptions::new().create(true).write(true).open(path).await
}

pub async fn seek(path: &str, pos: SeekFrom) -> Result<u64> {
    let mut file = open(path).await?;
    file.seek(pos).await
}

pub async fn read_exact_at(path: &str, buf: &mut [u8], offset: u64) -> Result<()> {
    let mut file = open(path).await?;
    file.seek(SeekFrom::Start(offset)).await?;
    let size = file.read_exact(buf).await?;
    assert_eq!(buf.len(), size);
    Ok(())
}

pub async fn folder_size<P>(path: P) -> Result<u64>
where
    P: Into<PathBuf> + AsRef<Path>,
{
    let mut total_size = 0;
    let mut queue: VecDeque<PathBuf> = VecDeque::new();
    queue.push_back(path.into());

    while let Some(current_path) = queue.pop_front() {
        let mut entries = read_dir(&current_path).await?;

        while let Some(entry) = entries.next_entry().await? {
            let metadata = entry.metadata().await?;

            if metadata.is_file() {
                total_size += metadata.len();
            } else if metadata.is_dir() {
                queue.push_back(entry.path());
            }
        }
    }
    Ok(total_size)
}
