#![allow(dead_code)]
#![allow(unused_variables)]
mod api;
mod crc;
mod disk_location;

pub mod erasure_coding;
mod errors;
pub use errors::{NeedleError, VolumeError};

mod file_id;
pub use file_id::FileId;

mod needle;
pub use needle::{
    read_index_entry, walk_index_file, MemoryNeedleValueMap, Needle, NeedleMapType, NeedleMapper,
    NeedleValue, NeedleValueMap,
};

mod server;
pub use server::StorageServer;

mod store;

mod types;
pub use types::{NeedleId, VolumeId};

mod ttl;
pub use ttl::Ttl;

mod version;
pub use version::CURRENT_VERSION;

mod volume;
pub use volume::{
    vacuum::{batch_vacuum_volume_check, batch_vacuum_volume_commit, batch_vacuum_volume_compact},
    ReplicaPlacement, VolumeInfo,
};

pub const BUFFER_SIZE_LIMIT: usize = 2 * 1024 * 1024;
