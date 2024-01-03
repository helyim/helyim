use std::result::Result;

use helyim_fs::File;

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

pub async fn check_volume_data_integrity(
    volume: &Volume,
    index_file: &File,
) -> Result<(), VolumeError> {
    let index_size = verify_index_file_integrity(index_file)?;
    if index_size == 0 {
        return Ok(());
    }
    let last_index_entry =
        read_index_entry_at_offset(index_file, index_size - NEEDLE_INDEX_SIZE as u64).await?;
    let (key, offset, size) = read_index_entry(&last_index_entry);
    if offset == 0 || size.is_deleted() {
        return Ok(());
    }
    let version = volume.version();

    verify_needle_integrity(volume.data_file()?, version, key, offset, size).await
}

pub async fn read_index_entry_at_offset(
    index_file: &File,
    offset: u64,
) -> Result<Vec<u8>, VolumeError> {
    let buf = vec![0u8; NEEDLE_INDEX_SIZE as usize];
    let (read, buf) = index_file.read_exact_at(buf, offset).await;
    read?;
    Ok(buf)
}

async fn verify_needle_integrity(
    data_file: &File,
    version: Version,
    key: NeedleId,
    offset: Offset,
    size: Size,
) -> Result<(), VolumeError> {
    let mut needle = Needle::default();
    needle.read_data(data_file, offset, size, version).await?;
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
    use helyim_fs::OpenOptions;
    use rand::random;
    use tempfile::Builder;

    use crate::storage::{
        crc,
        volume::{checking::check_volume_data_integrity, Volume},
        FileId, Needle, NeedleMapType, ReplicaPlacement, Ttl,
    };

    #[tokio::test]
    pub async fn test_check_volume_data_integrity() {
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
        .await
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
            volume.write_needle(&mut needle).await.unwrap();
        }

        let index_file = OpenOptions::new()
            .read(true)
            .open(volume.index_filename())
            .unwrap();

        assert!(check_volume_data_integrity(&volume, &index_file)
            .await
            .is_ok());
    }
}
