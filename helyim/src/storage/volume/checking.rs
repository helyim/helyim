use std::{fs::File, os::unix::fs::FileExt, result::Result};

use crate::storage::{
    index_entry,
    needle::{NEEDLE_INDEX_SIZE, NEEDLE_PADDING_SIZE},
    types::{Offset, Size},
    version::Version,
    volume::Volume,
    Needle, NeedleId, VolumeError,
};

pub fn verify_index_file_integrity(index_file: &File) -> Result<u64, VolumeError> {
    let meta = index_file.metadata()?;
    let size = meta.len();
    if size % NEEDLE_PADDING_SIZE as u64 != 0 {
        return Err(VolumeError::DataIntegrity(format!(
            "index file's size is {size} bytes, maybe corrupted"
        )));
    }
    Ok(size)
}

pub fn check_volume_data_integrity(
    volume: &mut Volume,
    index_file: &File,
) -> Result<(), VolumeError> {
    let index_size = verify_index_file_integrity(index_file)?;
    if index_size == 0 {
        return Ok(());
    }
    let last_index_entry =
        read_index_entry_at_offset(index_file, index_size - NEEDLE_INDEX_SIZE as u64)?;
    let (key, offset, size) = index_entry(&last_index_entry);
    if offset == 0 || size.is_deleted() {
        return Ok(());
    }
    let version = volume.version();
    verify_needle_integrity(volume.file_mut()?, version, key, offset, size)
}

pub fn read_index_entry_at_offset(index_file: &File, offset: u64) -> Result<Vec<u8>, VolumeError> {
    let mut buf = vec![0u8; NEEDLE_INDEX_SIZE as usize];
    index_file.read_exact_at(&mut buf, offset)?;
    Ok(buf)
}

fn verify_needle_integrity(
    data_file: &mut File,
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

    use crate::storage::{
        crc,
        volume::{checking::check_volume_data_integrity, Volume},
        Needle, NeedleMapType, ReplicaPlacement, Ttl,
    };

    #[tokio::test]
    pub async fn test_check_volume_data_integrity() {
        let mut volume = Volume::new(
            FastStr::from_static_str("/tmp/"),
            FastStr::empty(),
            1,
            NeedleMapType::NeedleMapInMemory,
            ReplicaPlacement::default(),
            Ttl::default(),
            0,
        )
        .unwrap();

        let fid = "1b1f52120";
        let data = Bytes::from_static(b"Hello World");
        let checksum = crc::checksum(&data);
        let mut needle = Needle {
            data,
            checksum,
            ..Default::default()
        };
        needle.parse_path(fid).unwrap();
        volume.write_needle(needle).unwrap();

        let index_file = std::fs::OpenOptions::new()
            .read(true)
            .open(volume.index_filename())
            .unwrap();

        assert!(check_volume_data_integrity(&mut volume, &index_file).is_ok());
    }
}
