use faststr::FastStr;
use serde::{Deserialize, Serialize};

use crate::{
    errors::Result,
    storage::{ttl::Ttl, version::Version, volume::ReplicaPlacement, VolumeId},
};

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
    pub fn new(m: helyim_proto::VolumeInformationMessage) -> Result<VolumeInfo> {
        let rp = ReplicaPlacement::from_u8(m.replica_placement as u8)?;
        Ok(VolumeInfo {
            id: m.id as VolumeId,
            size: m.size,
            collection: FastStr::new(m.collection),
            file_count: m.file_count as i64,
            delete_count: m.delete_count as i64,
            delete_bytes: m.deleted_bytes,
            read_only: m.read_only,
            version: m.version as Version,
            ttl: Ttl::from(m.ttl),
            replica_placement: rp,
        })
    }
}
