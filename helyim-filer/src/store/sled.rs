use async_trait::async_trait;
use tracing::error;

use crate::{
    entry::Entry,
    filer::{FilerError, FilerStore},
};

pub struct Sled {
    db: sled::Db,
}

impl Sled {
    pub fn new() -> Result<Sled, sled::Error> {
        Ok(Self {
            db: sled::open("/tmp/sled.db")?,
        })
    }
}

#[async_trait]
impl FilerStore for Sled {
    fn name(&self) -> &str {
        "sled"
    }

    async fn initialize(&self) -> Result<(), FilerError> {
        Ok(())
    }

    async fn insert_entry(&self, entry: &Entry) -> Result<(), FilerError> {
        let key = entry.full_path.as_bytes();
        let value = bincode::serialize(entry)?;
        self.db.insert(key, value)?;
        Ok(())
    }

    async fn update_entry(&self, entry: &Entry) -> Result<(), FilerError> {
        self.insert_entry(entry).await
    }

    async fn find_entry(&self, path: &str) -> Result<Option<Entry>, FilerError> {
        let entry = match self.db.get(path.as_bytes()) {
            Ok(Some(entry)) => entry,
            Ok(None) => return Ok(None),
            Err(err) => {
                error!("find entry: get {path} error: {err}");
                return Err(FilerError::Sled(err));
            }
        };

        let entry: Entry = bincode::deserialize(entry.as_ref())?;
        Ok(Some(entry))
    }

    async fn delete_entry(&self, path: &str) -> Result<(), FilerError> {
        self.db.remove(path.as_bytes())?;
        Ok(())
    }

    async fn list_directory_entries(
        &self,
        dir_path: &str,
        start_filename: &str,
        include_start_file: bool,
        mut limit: u32,
    ) -> Result<Vec<Entry>, FilerError> {
        let start_from = if start_filename.is_empty() {
            dir_path.to_string()
        } else {
            format!("{dir_path}/{start_filename}")
        };

        let mut entries = vec![];
        let mut iter = self.db.scan_prefix(start_from);
        while let Some(Ok((k, v))) = iter.next() {
            if entries.len() >= limit as usize {
                break;
            }
            if !include_start_file && start_filename.as_bytes() == k.as_ref() {
                continue;
            }
            if limit > 1 {
                limit -= 1;
            } else {
                break;
            }
            let entry: Entry = bincode::deserialize(v.as_ref())?;
            entries.push(entry);
        }
        Ok(entries)
    }

    fn begin_transaction(&self) -> Result<(), FilerError> {
        Ok(())
    }

    fn commit_transaction(&self) -> Result<(), FilerError> {
        Ok(())
    }

    fn rollback_transaction(&self) -> Result<(), FilerError> {
        Ok(())
    }
}
