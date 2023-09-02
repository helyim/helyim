use std::{fs, io::Write, os::unix::fs::OpenOptionsExt};

use crate::{
    errors::Result,
    storage::{
        needle::NEEDLE_PADDING_SIZE,
        needle_map::walk_index_file,
        volume::{scan_volume_file, Volume, SUPER_BLOCK_SIZE},
        Needle, NeedleMapper, NeedleValue,
    },
    util::time::now,
};

impl Volume {
    pub fn garbage_level(&self) -> f64 {
        self.needle_mapper.deleted_bytes() as f64 / self.content_size() as f64
    }

    pub fn copy_data_and_generate_index_file(
        &mut self,
        dst_name: &str,
        idx_name: &str,
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

    pub fn copy_data_based_on_index_file(&mut self, dst_name: &str, idx_name: &str) -> Result<()> {
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
