use std::{
    collections::HashMap,
    fs,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    os::unix::fs::OpenOptionsExt,
};

use bytes::BufMut;
use helyim_proto::{
    VacuumVolumeCheckRequest, VacuumVolumeCleanupRequest, VacuumVolumeCommitRequest,
    VacuumVolumeCompactRequest,
};
use tracing::{debug, error, info, warn};

use crate::{
    errors::{Error, Result},
    storage::{
        needle::{read_needle_blob, NEEDLE_INDEX_SIZE, NEEDLE_PADDING_SIZE},
        needle_map::{index_entry, walk_index_file},
        volume::{
            read_index_entry_at_offset, scan_volume_file, verify_index_file_integrity, SuperBlock,
            Volume, IDX_FILE_SUFFIX, SUPER_BLOCK_SIZE,
        },
        Needle, NeedleMapper, NeedleValue, VolumeId,
    },
    topology::{volume_layout::VolumeLayout, DataNodeEventTx},
    util::time::now,
};

impl Volume {
    pub fn makeup_diff(
        &self,
        new_data_filename: &str,
        new_idx_filename: &str,
        old_data_filename: &str,
        old_idx_filename: &str,
    ) -> Result<()> {
        let old_idx_file = fs::OpenOptions::new().read(true).open(old_idx_filename)?;
        let mut old_data_file = fs::OpenOptions::new().read(true).open(old_data_filename)?;

        let index_size = verify_index_file_integrity(&old_idx_file)?;
        if index_size == 0 || index_size <= self.last_compact_index_offset {
            return Ok(());
        }

        let old_compact_revision = fetch_compact_revision_from_data_file(&mut old_data_file)?;
        if old_compact_revision != self.last_compact_revision {
            return Err(Error::String(format!(
                "current old data file's compact revision {old_compact_revision} is not the \
                 expected one {}",
                self.last_compact_revision
            )));
        }

        let mut incremented_has_updated_index_entry = HashMap::new();

        {
            let mut idx_offset = index_size as i64 - NEEDLE_INDEX_SIZE as i64;
            loop {
                if idx_offset >= self.last_compact_index_offset as i64 {
                    let idx_entry = read_index_entry_at_offset(&old_idx_file, idx_offset as u64)?;
                    let (key, offset, size) = index_entry(&idx_entry);
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
            let mut new_idx_file = fs::OpenOptions::new()
                .write(true)
                .read(true)
                .mode(0o644)
                .open(new_idx_filename)?;
            let mut new_data_file = fs::OpenOptions::new()
                .write(true)
                .read(true)
                .mode(0o644)
                .open(new_data_filename)?;

            let new_compact_revision = fetch_compact_revision_from_data_file(&mut new_data_file)?;
            if old_compact_revision + 1 != new_compact_revision {
                return Err(Error::String(format!(
                    "old data file {}'s compact revision is {old_compact_revision} while new data \
                     file {}'s compact revision is {new_compact_revision}",
                    old_data_filename, new_data_filename
                )));
            }

            let mut index_entry_buf = [0u8; 16];
            for (key, value) in incremented_has_updated_index_entry {
                debug!(
                    "incremented index entry -> key: {key}, offset: {}, size: {}",
                    value.offset, value.size
                );
                (&mut index_entry_buf[0..8]).put_u64(key);
                (&mut index_entry_buf[8..12]).put_u32(value.offset);
                (&mut index_entry_buf[12..16]).put_i32(value.size.0);

                let mut offset = new_data_file.seek(SeekFrom::End(0))?;
                if offset % NEEDLE_PADDING_SIZE as u64 != 0 {
                    offset =
                        offset + (NEEDLE_PADDING_SIZE as u64 - offset % NEEDLE_PADDING_SIZE as u64);
                    offset = self.file()?.seek(SeekFrom::Start(offset))?;
                }

                if value.offset != 0 && value.size != 0 {
                    let needle_bytes =
                        read_needle_blob(&mut old_data_file, value.offset, value.size)?;
                    new_data_file.write_all(&needle_bytes)?;
                    (&mut index_entry_buf[8..12]).put_u32(offset as u32 / NEEDLE_PADDING_SIZE);
                } else {
                    let mut fake_del_needle = Needle {
                        id: key,
                        cookie: 0x12345678,
                        ..Default::default()
                    };
                    let version = self.version();
                    fake_del_needle.append(&mut new_data_file, version)?;
                    (&mut index_entry_buf[8..12]).put_u32(0);
                }

                new_idx_file.seek(SeekFrom::End(0))?;
                new_idx_file.write_all(&index_entry_buf)?;
            }
        }

        Ok(())
    }

    pub fn copy_data_and_generate_index_file(
        &mut self,
        compact_data_filename: String,
        compact_index_filename: String,
    ) -> Result<()> {
        let mut compact_data_file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .mode(0o644)
            .open(compact_data_filename)?;
        let compact_index_file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o644)
            .open(compact_index_filename)?;

        let mut compact_nm = NeedleMapper::new(self.id, self.needle_map_type);
        compact_nm.load_idx_file(compact_index_file)?;

        let mut new_offset = SUPER_BLOCK_SIZE as u32;
        let now = now().as_millis() as u64;
        let version = self.version();

        let mut dst = compact_data_file.try_clone()?;
        scan_volume_file(
            self.dir.clone(),
            self.collection.clone(),
            self.id,
            self.needle_map_type,
            true,
            |super_block| -> Result<()> {
                super_block.compact_revision += 1;
                compact_data_file.write_all(&super_block.as_bytes())?;
                Ok(())
            },
            |needle, offset| -> Result<()> {
                if needle.has_ttl()
                    && now >= needle.last_modified + self.super_block.ttl.minutes() as u64 * 60
                {
                    return Ok(());
                }
                if let Some(nv) = self.needle_mapper.get(needle.id) {
                    if nv.offset * NEEDLE_PADDING_SIZE == offset && nv.size > 0 {
                        let nv = NeedleValue {
                            offset: new_offset / NEEDLE_PADDING_SIZE,
                            size: needle.size,
                        };
                        compact_nm.set(needle.id, nv)?;
                        needle.append(&mut dst, version)?;
                        new_offset += needle.disk_size() as u32;
                    }
                }
                Ok(())
            },
        )?;
        Ok(())
    }

    pub fn copy_data_based_on_index_file(
        &mut self,
        compact_data_filename: String,
        compact_index_filename: String,
    ) -> Result<()> {
        let mut compact_data_file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o644)
            .open(compact_data_filename)?;
        let compact_index_file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o644)
            .open(compact_index_filename)?;

        let old_idx_file = fs::OpenOptions::new()
            .read(true)
            .mode(0o644)
            .open(format!("{}.{IDX_FILE_SUFFIX}", self.filename()))?;

        let mut compact_nm = NeedleMapper::new(self.id, self.needle_map_type);
        compact_nm.load_idx_file(compact_index_file)?;

        let now = now().as_millis() as u64;

        self.super_block.compact_revision += 1;
        compact_data_file.write_all(&self.super_block.as_bytes())?;
        let mut new_offset = SUPER_BLOCK_SIZE as u32;

        walk_index_file(&old_idx_file, |key, offset, size| -> Result<()> {
            if offset == 0 {
                return Ok(());
            }

            let nv = match self.needle_mapper.get(key) {
                Some(nv) => nv,
                None => return Ok(()),
            };

            let mut needle = Needle::default();
            let version = self.version();

            needle.read_data(self.file_mut()?, offset, size, version)?;

            if needle.has_ttl()
                && now >= needle.last_modified + self.super_block.ttl.minutes() as u64 * 60
            {
                return Ok(());
            }

            if nv.offset == offset && nv.size > 0 {
                let nv = NeedleValue {
                    offset: new_offset / NEEDLE_PADDING_SIZE,
                    size: needle.size,
                };
                compact_nm.set(needle.id, nv)?;
                needle.append(&mut compact_data_file, version)?;
                new_offset += needle.disk_size() as u32;
            }

            Ok(())
        })?;

        Ok(())
    }
}

fn fetch_compact_revision_from_data_file(file: &mut File) -> Result<u16> {
    let mut buf = [0u8; SUPER_BLOCK_SIZE];
    file.read_exact(&mut buf)?;
    let sb = SuperBlock::parse(buf)?;
    Ok(sb.compact_revision)
}

pub async fn batch_vacuum_volume_check(
    volume_id: VolumeId,
    data_nodes: &[DataNodeEventTx],
    garbage_ratio: f64,
) -> Result<bool> {
    let mut check_success = true;
    for data_node_tx in data_nodes {
        let request = VacuumVolumeCheckRequest { volume_id };
        match data_node_tx.vacuum_volume_check(request).await {
            Ok(response) => {
                info!("check volume {volume_id} success.");
                check_success = response.garbage_ratio > garbage_ratio;
            }
            Err(err) => {
                error!("check volume {volume_id} failed, {err}");
                check_success = false;
            }
        }
    }
    Ok(check_success)
}

pub async fn batch_vacuum_volume_compact(
    volume_layout: &VolumeLayout,
    volume_id: VolumeId,
    data_nodes: &[DataNodeEventTx],
    preallocate: u64,
) -> Result<bool> {
    volume_layout.remove_from_writable(volume_id);
    let mut compact_success = true;
    for data_node_tx in data_nodes {
        let request = VacuumVolumeCompactRequest {
            volume_id,
            preallocate,
        };
        match data_node_tx.vacuum_volume_compact(request).await {
            Ok(_) => {
                info!("compact volume {volume_id} success.");
                compact_success = true;
            }
            Err(err) => {
                error!("compact volume {volume_id} failed, {err}");
                compact_success = false;
            }
        }
    }
    Ok(compact_success)
}

pub async fn batch_vacuum_volume_commit(
    volume_layout: &VolumeLayout,
    volume_id: VolumeId,
    data_nodes: &[DataNodeEventTx],
) -> Result<bool> {
    let mut commit_success = true;
    for data_node_tx in data_nodes {
        let request = VacuumVolumeCommitRequest { volume_id };
        match data_node_tx.vacuum_volume_commit(request).await {
            Ok(response) => {
                if response.is_read_only {
                    warn!("volume {volume_id} is read only, will not commit it.");
                    commit_success = false;
                } else {
                    info!("commit volume {volume_id} success.");
                    commit_success = true;
                    volume_layout
                        .set_volume_available(volume_id, data_node_tx)
                        .await?;
                }
            }
            Err(err) => {
                error!("commit volume {volume_id} failed, {err}");
                commit_success = false;
            }
        }
    }
    Ok(commit_success)
}

#[allow(dead_code)]
async fn batch_vacuum_volume_cleanup(
    volume_id: VolumeId,
    data_nodes: &[DataNodeEventTx],
) -> Result<bool> {
    let mut cleanup_success = true;
    for data_node_tx in data_nodes {
        let request = VacuumVolumeCleanupRequest { volume_id };
        match data_node_tx.vacuum_volume_cleanup(request).await {
            Ok(_) => {
                info!("cleanup volume {volume_id} success.");
                cleanup_success = true;
            }
            Err(_err) => {
                cleanup_success = false;
            }
        }
    }
    Ok(cleanup_success)
}
