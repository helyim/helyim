use std::{
    collections::HashMap,
    fs,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    os::unix::fs::OpenOptionsExt,
};

use bytes::BufMut;
use tracing::error;

use crate::{
    errors::{Error, Result},
    storage::{
        needle::{read_needle_blob, NEEDLE_INDEX_SIZE, NEEDLE_PADDING_SIZE},
        needle_map::{index_entry, walk_index_file},
        volume::{
            read_index_entry_at_offset, scan_volume_file, verify_index_file_integrity, SuperBlock,
            Volume, SUPER_BLOCK_SIZE,
        },
        Needle, NeedleMapper, NeedleValue,
    },
    util::time::now,
};

impl Volume {
    pub fn garbage_level(&self) -> f64 {
        self.needle_mapper.deleted_bytes() as f64 / self.content_size() as f64
    }

    pub fn compact(&mut self, preallocate: u64) -> Result<()> {
        let file_path = self.file_name();
        self.last_compact_index_offset = self.needle_mapper.index_file_size()?;
        self.last_compact_revision = self.super_block.compact_revision;
        self.copy_data_and_generate_index_file(
            format!("{}.cpd", file_path),
            format!("{}.cpx", file_path),
            preallocate,
        )
    }

    pub fn compact2(&mut self) -> Result<()> {
        let file_path = self.file_name();
        self.copy_data_based_on_index_file(
            format!("{}.cpd", file_path),
            format!("{}.cpx", file_path),
        )
    }

    pub fn commit_compact(&mut self) -> Result<()> {
        let compact_data_filename = format!("{}.cpd", self.file_name());
        let compact_index_filename = format!("{}.cpx", self.file_name());
        let data_filename = format!("{}.dat", self.file_name());
        let index_filename = format!("{}.idx", self.file_name());
        match self.makeup_diff(
            compact_data_filename.clone(),
            compact_index_filename.clone(),
            data_filename.clone(),
            index_filename.clone(),
        ) {
            Ok(()) => {
                fs::rename(compact_data_filename, data_filename)?;
                fs::rename(compact_index_filename, index_filename)?;
            }
            Err(err) => {
                error!("makeup diff in commit compact failed, {err}");
                fs::remove_file(compact_data_filename)?;
                fs::remove_file(compact_index_filename)?;
            }
        }

        self.load(false, true)
    }

    pub fn cleanup_compact(&mut self) -> Result<()> {
        fs::remove_file(format!("{}.cpd", self.file_name()))?;
        fs::remove_file(format!("{}.cpx", self.file_name()))?;
        Ok(())
    }

    pub fn makeup_diff(
        &self,
        new_data_filename: String,
        new_idx_filename: String,
        old_data_filename: String,
        old_idx_filename: String,
    ) -> Result<()> {
        let old_idx_fie = fs::OpenOptions::new().open(old_idx_filename.as_str())?;
        let mut old_data_fie = fs::OpenOptions::new().open(old_data_filename.as_str())?;

        let index_size = verify_index_file_integrity(&old_idx_fie)?;
        if index_size == 0 || index_size <= self.last_compact_index_offset {
            return Ok(());
        }

        let old_compact_revision = fetch_compact_revision_from_data_file(&mut old_data_fie)?;
        if old_compact_revision != self.last_compact_revision {
            return Err(Error::String(format!(
                "current old data file's compact revision {old_compact_revision} is not the \
                 expected one {}",
                self.last_compact_revision
            )));
        }

        struct KeyField {
            offset: u32,
            size: u32,
        }

        let mut increment_has_updated_index_entry = HashMap::new();

        {
            let mut idx_offset = index_size - NEEDLE_INDEX_SIZE as u64;
            loop {
                if idx_offset >= self.last_compact_index_offset {
                    let idx_entry = read_index_entry_at_offset(&old_idx_fie, idx_offset)?;
                    let (key, offset, size) = index_entry(&idx_entry);
                    increment_has_updated_index_entry
                        .entry(key)
                        .or_insert(KeyField { offset, size });

                    idx_offset -= NEEDLE_INDEX_SIZE as u64;
                } else {
                    break;
                }
            }
        }

        if !increment_has_updated_index_entry.is_empty() {
            let mut new_idx_file = fs::OpenOptions::new()
                .write(true)
                .read(true)
                .mode(0o644)
                .open(new_idx_filename.as_str())?;
            let mut new_data_file = fs::OpenOptions::new()
                .write(true)
                .read(true)
                .mode(0o644)
                .open(new_data_filename.as_str())?;

            let new_compact_revision = fetch_compact_revision_from_data_file(&mut new_data_file)?;
            if old_compact_revision + 1 != new_compact_revision {
                return Err(Error::String(format!(
                    "old data file {}'s compact revision is {old_compact_revision} while new data \
                     file {}'s compact revision is {new_compact_revision}",
                    old_data_filename, new_data_filename
                )));
            }

            let mut index_entry_buf = vec![0u8; 16];
            for (key, value) in increment_has_updated_index_entry {
                index_entry_buf.put_u64(key);
                index_entry_buf.put_u32(value.offset);
                index_entry_buf.put_u32(value.size);

                let mut offset = new_data_file.seek(SeekFrom::End(0))?;
                if offset % NEEDLE_PADDING_SIZE as u64 != 0 {
                    offset =
                        offset + (NEEDLE_PADDING_SIZE as u64 - offset % NEEDLE_PADDING_SIZE as u64);
                    offset = self.file()?.seek(SeekFrom::Start(offset))?;
                }

                if value.offset != 0 && value.size != 0 {
                    let needle_bytes = read_needle_blob(
                        &mut old_data_fie,
                        value.offset * NEEDLE_PADDING_SIZE,
                        value.size,
                    )?;
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
        dst_name: String,
        idx_name: String,
        _preallocate: u64,
    ) -> Result<()> {
        let mut dst_file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .mode(0o644)
            .open(dst_name)?;
        let idx_file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o644)
            .open(idx_name)?;

        let mut nm = NeedleMapper::default();
        nm.load_idx_file(&idx_file)?;

        let mut new_offset = SUPER_BLOCK_SIZE as u32;
        let now = now().as_millis() as u64;
        let version = self.version();

        let mut dst = dst_file.try_clone()?;
        scan_volume_file(
            self.dir.clone(),
            self.collection.clone(),
            self.id,
            self.needle_map_type,
            true,
            |super_block| -> Result<()> {
                super_block.compact_revision += 1;
                dst_file.write_all(&super_block.as_bytes())?;
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
                        nm.set(needle.id, nv);
                        nm.append_to_index_file(needle.id, nv)?;

                        needle.append(&mut dst, version)?;
                        new_offset += needle.disk_size();
                    }
                }
                Ok(())
            },
        )?;

        Ok(())
    }

    pub fn copy_data_based_on_index_file(
        &mut self,
        dst_name: String,
        idx_name: String,
    ) -> Result<()> {
        let mut dst_file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o644)
            .open(dst_name)?;
        let idx_file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o644)
            .open(idx_name)?;

        let old_idx_file = fs::OpenOptions::new()
            .read(true)
            .mode(0o644)
            .open(format!("{}.idx", self.file_name()))?;

        let mut nm = NeedleMapper::default();
        nm.load_idx_file(&idx_file)?;

        let now = now().as_millis() as u64;

        self.super_block.compact_revision += 1;
        dst_file.write_all(&self.super_block.as_bytes())?;
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
                nm.set(needle.id, nv);
                nm.append_to_index_file(needle.id, nv)?;

                needle.append(&mut dst_file, version)?;
                new_offset += needle.disk_size();
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
