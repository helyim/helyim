use std::{io, io::ErrorKind};

use faststr::FastStr;
use helyim_common::{
    ttl::Ttl,
    types::{ReplicaPlacement, VolumeId},
    version::Version,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct VolumeInfo {
    pub id: VolumeId,
    pub size: u64,
    pub replica_placement: ReplicaPlacement,
    pub ttl: Ttl,
    pub collection: FastStr,
    pub version: Version,
    pub file_count: i64,
    pub delete_count: i64,
    pub delete_bytes: u64,
    pub read_only: bool,
}

impl VolumeInfo {
    pub fn new(
        m: &helyim_proto::directory::VolumeInformationMessage,
    ) -> Result<VolumeInfo, io::Error> {
        let rp = ReplicaPlacement::from_u8(m.replica_placement as u8)?;
        Ok(VolumeInfo {
            id: m.id as VolumeId,
            size: m.size,
            collection: FastStr::new(&m.collection),
            file_count: m.file_count as i64,
            delete_count: m.delete_count as i64,
            delete_bytes: m.deleted_bytes,
            read_only: m.read_only,
            version: m.version as Version,
            ttl: Ttl::from_u32(m.ttl).map_err(|err| {
                io::Error::new(ErrorKind::InvalidData, format!("Ttl error: {err}"))
            })?,
            replica_placement: rp,
        })
    }

    pub fn new_from_short(
        m: &helyim_proto::directory::VolumeShortInformationMessage,
    ) -> Result<VolumeInfo, io::Error> {
        let rp = ReplicaPlacement::from_u8(m.replica_placement as u8)?;
        Ok(VolumeInfo {
            id: m.id as VolumeId,
            collection: FastStr::new(&m.collection),
            version: m.version as Version,
            ttl: Ttl::from_u32(m.ttl).map_err(|err| {
                io::Error::new(ErrorKind::InvalidData, format!("Ttl error: {err}"))
            })?,
            replica_placement: rp,
            ..Default::default()
        })
    }
}
