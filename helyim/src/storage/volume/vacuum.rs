use std::{
    collections::HashMap,
    io::{BufReader, Read},
    os::unix::fs::OpenOptionsExt,
};

use bytes::BufMut;
use helyim_fs::{File, OpenOptions};
use helyim_proto::{
    VacuumVolumeCheckRequest, VacuumVolumeCleanupRequest, VacuumVolumeCommitRequest,
    VacuumVolumeCompactRequest,
};
use tokio::fs;
use tracing::{debug, error, info};

use crate::{
    storage::{
        needle::{
            read_index_entry, read_needle_blob, NeedleMapper, NEEDLE_INDEX_SIZE,
            NEEDLE_PADDING_SIZE,
        },
        volume::{
            checking::{read_index_entry_at_offset, verify_index_file_integrity},
            SuperBlock, Volume, COMPACT_DATA_FILE_SUFFIX, COMPACT_IDX_FILE_SUFFIX,
            DATA_FILE_SUFFIX, IDX_FILE_SUFFIX, SUPER_BLOCK_SIZE,
        },
        Needle, NeedleError, NeedleValue, VolumeError, VolumeId,
    },
    topology::{volume_layout::VolumeLayoutRef, DataNodeRef},
    util::time::now,
};

impl Volume {
    pub async fn garbage_level(&self) -> f64 {
        if self.content_size().await == 0 {
            return 0.0;
        }
        self.deleted_bytes().await as f64 / self.content_size().await as f64
    }

    pub async fn compact2(&self) -> Result<(), VolumeError> {
        let filename = self.filename();
        self.set_last_compact_index_offset(self.index_file_size().await?);
        self.set_last_compact_revision(self.super_block.compact_revision());
        self.set_readonly(true);
        self.copy_data_based_on_index_file(
            format!("{}.{COMPACT_DATA_FILE_SUFFIX}", filename),
            format!("{}.{COMPACT_IDX_FILE_SUFFIX}", filename),
        )
        .await?;
        info!("compact {filename} success");
        Ok(())
    }

    pub async fn commit_compact(&mut self) -> Result<(), VolumeError> {
        let filename = self.filename();
        let compact_data_filename = format!("{}.{COMPACT_DATA_FILE_SUFFIX}", filename);
        let compact_index_filename = format!("{}.{COMPACT_IDX_FILE_SUFFIX}", filename);
        let data_filename = format!("{}.{DATA_FILE_SUFFIX}", filename);
        let index_filename = format!("{}.{IDX_FILE_SUFFIX}", filename);

        {
            let _lock = self.data_file_lock.write().await;
            info!("starting to commit compaction, filename: {compact_data_filename}");
            match self
                .makeup_diff(
                    &compact_data_filename,
                    &compact_index_filename,
                    &data_filename,
                    &index_filename,
                )
                .await
            {
                Ok(()) => {
                    fs::rename(&compact_data_filename, data_filename).await?;
                    fs::rename(compact_index_filename, index_filename).await?;
                    info!(
                        "makeup diff in commit compaction success, filename: \
                         {compact_data_filename}"
                    );
                }
                Err(err) => {
                    error!("makeup diff in commit compaction failed, {err}");
                    fs::remove_file(compact_data_filename).await?;
                    fs::remove_file(compact_index_filename).await?;
                }
            }

            self.data_file = None;
            self.needle_mapper = None;
        }
        self.load(false, true).await
    }

    pub async fn cleanup_compact(&self) -> Result<(), std::io::Error> {
        let filename = self.filename();
        fs::remove_file(format!("{}.{COMPACT_DATA_FILE_SUFFIX}", filename)).await?;
        fs::remove_file(format!("{}.{COMPACT_IDX_FILE_SUFFIX}", filename)).await?;
        info!("cleanup compaction success, filename: {filename}");
        Ok(())
    }

    async fn makeup_diff(
        &self,
        new_data_filename: &str,
        new_idx_filename: &str,
        old_data_filename: &str,
        old_idx_filename: &str,
    ) -> Result<(), VolumeError> {
        let old_idx_file = OpenOptions::new().read(true).open(old_idx_filename)?;
        let old_data_file = OpenOptions::new().read(true).open(old_data_filename)?;

        let index_size = verify_index_file_integrity(&old_idx_file)?;
        if index_size == 0 || index_size <= self.last_compact_index_offset() {
            return Ok(());
        }

        let old_compact_revision = fetch_compact_revision_from_data_file(&old_data_file).await?;
        if old_compact_revision != self.last_compact_revision() {
            return Err(VolumeError::String(format!(
                "current old data file's compact revision {old_compact_revision} is not the \
                 expected one {}",
                self.last_compact_revision()
            )));
        }

        let mut incremented_has_updated_index_entry = HashMap::new();

        {
            let mut idx_offset = index_size as i64 - NEEDLE_INDEX_SIZE as i64;
            loop {
                if idx_offset >= self.last_compact_index_offset() as i64 {
                    let idx_entry =
                        read_index_entry_at_offset(&old_idx_file, idx_offset as u64).await?;
                    let (key, offset, size) = read_index_entry(&idx_entry);
                    incremented_has_updated_index_entry
                        .entry(key)
                        .or_insert(NeedleValue { offset, size });

                    idx_offset -= NEEDLE_INDEX_SIZE as i64;
                } else {
                    break;
                }
            }
        }

        if !incremented_has_updated_index_entry.is_empty() {
            let new_idx_file = OpenOptions::new()
                .write(true)
                .read(true)
                .mode(0o644)
                .open(new_idx_filename)?;
            let new_data_file = OpenOptions::new()
                .write(true)
                .read(true)
                .mode(0o644)
                .open(new_data_filename)?;

            let new_compact_revision =
                fetch_compact_revision_from_data_file(&new_data_file).await?;
            if old_compact_revision + 1 != new_compact_revision {
                return Err(VolumeError::String(format!(
                    "old data file {}'s compact revision is {old_compact_revision} while new data \
                     file {}'s compact revision is {new_compact_revision}",
                    old_data_filename, new_data_filename
                )));
            }

            let mut index_entry_buf = vec![0u8; 16];
            for (key, value) in incremented_has_updated_index_entry {
                debug!(
                    "incremented index entry -> key: {key}, offset: {}, size: {}",
                    value.offset, value.size
                );
                (&mut index_entry_buf[0..8]).put_u64(key);
                (&mut index_entry_buf[8..12]).put_u32(value.offset.0);
                (&mut index_entry_buf[12..16]).put_i32(value.size.0);

                let mut offset = new_data_file.metadata()?.len();
                if offset % NEEDLE_PADDING_SIZE as u64 != 0 {
                    offset =
                        offset + (NEEDLE_PADDING_SIZE as u64 - offset % NEEDLE_PADDING_SIZE as u64);
                }

                if value.offset != 0 && value.size != 0 {
                    let needle_bytes =
                        read_needle_blob(&old_data_file, value.offset, value.size).await?;
                    let (write, _) = new_data_file.write_all_at(needle_bytes, offset).await;
                    write?;
                    (&mut index_entry_buf[8..12]).put_u32(offset as u32 / NEEDLE_PADDING_SIZE);
                } else {
                    let mut fake_del_needle = Needle {
                        id: key,
                        cookie: 0x12345678,
                        ..Default::default()
                    };
                    let version = self.version();
                    fake_del_needle
                        .append(&new_data_file, offset, version)
                        .await?;
                    (&mut index_entry_buf[8..12]).put_u32(0);
                }

                let offset = new_idx_file.metadata()?.len();
                let (write, index_entry) = new_idx_file.write_all_at(index_entry_buf, offset).await;
                write?;
                index_entry_buf = index_entry;
            }
        }

        Ok(())
    }

    async fn copy_data_based_on_index_file(
        &self,
        compact_data_filename: String,
        compact_index_filename: String,
    ) -> Result<(), VolumeError> {
        let compact_data_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o644)
            .open(compact_data_filename)?;
        let compact_index_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o644)
            .open(compact_index_filename)?;

        let old_idx_file = std::fs::OpenOptions::new()
            .read(true)
            .mode(0o644)
            .open(format!("{}.{IDX_FILE_SUFFIX}", self.filename()))?;

        let mut compact_nm = NeedleMapper::new(self.id, self.needle_map_type);
        compact_nm.load_index_file(compact_index_file).await?;

        let now = now().as_millis() as u64;

        self.super_block.add_compact_revision(1);
        let (write, _) = compact_data_file
            .write_all_at(self.super_block.as_bytes(), 0)
            .await;
        write?;
        let mut new_offset = SUPER_BLOCK_SIZE as u64;

        // walk index file
        let len = old_idx_file.metadata()?.len();
        let mut reader = BufReader::new(old_idx_file);
        let mut buf: Vec<u8> = vec![0; 16];

        // if there is a not complete entry, will err
        for _ in 0..(len + 15) / 16 {
            reader.read_exact(&mut buf)?;

            let (key, offset, size) = read_index_entry(&buf);
            if offset == 0 {
                return Ok(());
            }

            let nv = match self
                .get_index(key)
                .map_err(|err| NeedleError::Box(err.into()))?
            {
                Some(nv) => nv,
                None => return Ok(()),
            };

            let mut needle = Needle::default();
            let version = self.version();

            needle
                .read_data(
                    self.data_file()
                        .map_err(|err| NeedleError::Box(err.into()))?,
                    offset,
                    size,
                    version,
                )
                .await?;

            if needle.has_ttl()
                && now >= needle.last_modified + self.super_block.ttl.minutes() as u64 * 60
            {
                return Ok(());
            }

            if nv.offset == offset && nv.size > 0 {
                let nv = NeedleValue {
                    offset: new_offset.into(),
                    size: needle.size,
                };
                compact_nm
                    .set(needle.id, nv)
                    .await
                    .map_err(|err| NeedleError::Box(err.into()))?;
                needle
                    .append(&compact_data_file, new_offset, version)
                    .await?;
                new_offset += needle.disk_size();
            }
        }

        Ok(())
    }
}

async fn fetch_compact_revision_from_data_file(file: &File) -> Result<u16, VolumeError> {
    let buf = vec![0u8; SUPER_BLOCK_SIZE];
    let (read, buf) = file.read_exact_at(buf, 0).await;
    read?;
    let sb = SuperBlock::parse(buf)?;
    Ok(sb.compact_revision())
}

pub async fn batch_vacuum_volume_check(
    volume_id: VolumeId,
    data_nodes: &[DataNodeRef],
    garbage_ratio: f64,
) -> bool {
    let mut need_vacuum = true;
    for data_node in data_nodes {
        let request = VacuumVolumeCheckRequest { volume_id };
        let response = data_node.vacuum_volume_check(request).await;
        match response {
            Ok(response) => {
                if response.garbage_ratio > 0.0 {
                    info!(
                        "check volume {}:{volume_id} success. garbage ratio is {}",
                        data_node.public_url, response.garbage_ratio
                    );
                }
                need_vacuum = response.garbage_ratio > garbage_ratio;
            }
            Err(err) => {
                error!(
                    "check volume {}:{volume_id} failed, {err}",
                    data_node.public_url
                );
                need_vacuum = false;
            }
        }
    }
    need_vacuum
}

pub async fn batch_vacuum_volume_compact(
    volume_layout: &VolumeLayoutRef,
    volume_id: VolumeId,
    data_nodes: &[DataNodeRef],
    preallocate: u64,
) -> bool {
    volume_layout.remove_from_writable(volume_id).await;
    let mut compact_success = true;
    for data_node in data_nodes {
        let request = VacuumVolumeCompactRequest {
            volume_id,
            preallocate,
        };
        let response = data_node.vacuum_volume_compact(request).await;
        match response {
            Ok(_) => {
                info!(
                    "compact volume {}:{volume_id} success.",
                    data_node.public_url
                );
                compact_success = true;
            }
            Err(err) => {
                error!(
                    "compact volume {}:{volume_id} failed, {err}",
                    data_node.public_url
                );
                compact_success = false;
            }
        }
    }
    compact_success
}

pub async fn batch_vacuum_volume_commit(
    volume_layout: &VolumeLayoutRef,
    volume_id: VolumeId,
    data_nodes: &[DataNodeRef],
) -> bool {
    let mut commit_success = true;
    let mut is_readonly = false;
    for data_node in data_nodes {
        let request = VacuumVolumeCommitRequest { volume_id };
        let response = data_node.vacuum_volume_commit(request).await;
        match response {
            Ok(response) => {
                if response.is_read_only {
                    is_readonly = true;
                }
                info!(
                    "commit volume {}:{volume_id} success.",
                    data_node.public_url
                );
            }
            Err(err) => {
                error!(
                    "commit volume {}:{volume_id} failed, {err}",
                    data_node.public_url
                );
                commit_success = false;
            }
        }
    }
    if commit_success {
        for data_node in data_nodes {
            volume_layout
                .set_volume_available(volume_id, data_node, is_readonly)
                .await;
        }
    }

    commit_success
}

#[allow(dead_code)]
async fn batch_vacuum_volume_cleanup(volume_id: VolumeId, data_nodes: &[DataNodeRef]) -> bool {
    let mut cleanup_success = true;
    for data_node in data_nodes {
        let request = VacuumVolumeCleanupRequest { volume_id };
        let response = data_node.vacuum_volume_cleanup(request).await;
        match response {
            Ok(_) => {
                info!(
                    "cleanup volume {}:{volume_id} success.",
                    data_node.public_url
                );
                cleanup_success = true;
            }
            Err(_err) => {
                cleanup_success = false;
            }
        }
    }
    cleanup_success
}
