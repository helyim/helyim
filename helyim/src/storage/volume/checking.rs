use std::{io::SeekFrom, result::Result};

use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};

use crate::storage::{
    index_entry,
    needle::{NEEDLE_INDEX_SIZE, NEEDLE_PADDING_SIZE},
    types::{Offset, Size},
    version::Version,
    volume::Volume,
    Needle, NeedleId, VolumeError,
};

pub async fn verify_index_file_integrity(index_file: &File) -> Result<u64, VolumeError> {
    let size = index_file.metadata().await?.len();
    if size % NEEDLE_INDEX_SIZE as u64 != 0 {
        return Err(VolumeError::DataIntegrity(format!(
            "index file's size is {size} bytes, maybe corrupted"
        )));
    }
    Ok(size)
}

pub async fn check_volume_data_integrity(
    volume: &mut Volume,
    index_file: &mut File,
) -> Result<(), VolumeError> {
    let index_size = verify_index_file_integrity(index_file).await?;
    if index_size == 0 {
        return Ok(());
    }
    let last_index_entry =
        read_index_entry_at_offset(index_file, index_size - NEEDLE_INDEX_SIZE as u64).await?;
    let (key, offset, size) = index_entry(&last_index_entry);
    if offset == 0 || size.is_deleted() {
        return Ok(());
    }
    let version = volume.version();
    verify_needle_integrity(
        volume.data_file()?,
        version,
        key,
        offset * NEEDLE_PADDING_SIZE,
        size,
    )
    .await
}

pub async fn read_index_entry_at_offset<F: AsyncReadExt + AsyncSeekExt + Unpin>(
    index_file: &mut F,
    offset: u64,
) -> Result<Vec<u8>, VolumeError> {
    let mut buf = vec![0u8; NEEDLE_INDEX_SIZE as usize];
    index_file.seek(SeekFrom::Start(offset)).await?;
    let size = index_file.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn verify_needle_integrity<F: AsyncReadExt + AsyncSeekExt + Unpin>(
    reader: &mut F,
    version: Version,
    key: NeedleId,
    offset: Offset,
    size: Size,
) -> Result<(), VolumeError> {
    let mut needle = Needle::default();
    needle.read_data(reader, offset, size, version).await?;
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

    use crate::{
        storage::{
            crc,
            volume::{checking::check_volume_data_integrity, Volume},
            FileId, Needle, NeedleMapType, ReplicaPlacement, Ttl,
        },
        util::file,
    };

    #[tokio::test]
    pub async fn test_check_volume_data_integrity() {
        let dir = Builder::new()
            .prefix("check_volume_data_integrity")
            .tempdir_in(".")
            .unwrap();
        let dir = FastStr::new(dir.path().to_str().unwrap());
        let mut volume = Volume::new(
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
            volume.write_needle(needle).await.unwrap();
        }

        let mut index_file = file::open(volume.index_filename()).await.unwrap();
        assert!(check_volume_data_integrity(&mut volume, &mut index_file)
            .await
            .is_ok());
    }
}
