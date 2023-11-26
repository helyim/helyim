use std::{
    collections::VecDeque,
    io::Result,
    path::{Path, PathBuf},
};

use tokio::fs::{read_dir, File, OpenOptions};

pub async fn open(path: &str) -> Result<File> {
    OpenOptions::new().read(true).open(path).await
}

pub async fn append(path: &str) -> Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .append(true)
        .open(path)
        .await
}

pub async fn create(path: &str) -> Result<File> {
    OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(path)
        .await
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
