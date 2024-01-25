use std::{fs::File, os::unix::fs::FileExt, result::Result};

use crate::storage::{
    needle::NEEDLE_INDEX_SIZE,
    read_index_entry,
    types::{Offset, Size},
    version::Version,
    volume::Volume,
    Needle, NeedleId, VolumeError,
};

pub fn verify_index_file_integrity(index_file: &File) -> Result<u64, VolumeError> {
    let meta = index_file.metadata()?;
    let size = meta.len();
    if size % NEEDLE_INDEX_SIZE as u64 != 0 {
        return Err(VolumeError::DataIntegrity(format!(
            "index file's size is {size} bytes, maybe corrupted"
        )));
    }
    Ok(size)
}

pub fn check_volume_data_integrity(volume: &Volume, index_file: &File) -> Result<(), VolumeError> {
    let index_size = verify_index_file_integrity(index_file)?;
    if index_size == 0 {
        return Ok(());
    }
    let last_index_entry =
        read_index_entry_at_offset(index_file, index_size - NEEDLE_INDEX_SIZE as u64)?;
    let (key, offset, size) = read_index_entry(&last_index_entry);
    if offset == 0 || size.is_deleted() {
        return Ok(());
    }
    let version = volume.version();
    verify_needle_integrity(volume.data_file()?, version, key, offset, size)
}

pub fn read_index_entry_at_offset(index_file: &File, offset: u64) -> Result<Vec<u8>, VolumeError> {
    let mut buf = vec![0u8; NEEDLE_INDEX_SIZE as usize];
    index_file.read_exact_at(&mut buf, offset)?;
    Ok(buf)
}

fn verify_needle_integrity(
    data_file: &File,
    version: Version,
    key: NeedleId,
    offset: Offset,
    size: Size,
) -> Result<(), VolumeError> {
    let mut needle = Needle::default();
    needle.read_data(data_file, offset, size, version)?;
    if needle.id != key {
        return Err(VolumeError::DataIntegrity(format!(
            "index key {key} does not match needle's id {}",
            needle.id
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use faststr::FastStr;
    use rand::random;
    use tempfile::Builder;

    use crate::storage::{
        crc,
        volume::{checking::check_volume_data_integrity, Volume},
        FileId, Needle, NeedleMapType, ReplicaPlacement, Ttl,
    };

    #[test]
    pub fn test_check_volume_data_integrity() {
        let dir = Builder::new()
            .prefix("check_volume_data_integrity")
            .tempdir_in(".")
            .unwrap();
        let dir = FastStr::new(dir.path().to_str().unwrap());
        let volume = Volume::new(
            dir,
            FastStr::empty(),
            1,
            NeedleMapType::NeedleMapInMemory,
            ReplicaPlacement::default(),
            Ttl::default(),
            0,
        )
        .unwrap();

        for i in 0..1000 {
            let fid = FileId::new(1, i, random::<u32>());
            let data = Bytes::from_static(b"Hello World");
            let checksum = crc::checksum(&data);
            let mut needle = Needle {
                data,
                checksum,
                ..Default::default()
            };
            needle
                .parse_path(&format!("{:x}{:08x}", fid.key, fid.hash))
                .unwrap();
            volume.write_needle(&mut needle).unwrap();
        }

        let index_file = std::fs::OpenOptions::new()
            .read(true)
            .open(volume.index_filename())
            .unwrap();

        assert!(check_volume_data_integrity(&volume, &index_file).is_ok());
    }
}
