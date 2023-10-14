#![allow(dead_code)]
#![allow(unused_variables)]
mod api;
mod crc;
mod disk_location;

mod errors;
pub use errors::{NeedleError, VolumeError};

mod file_id;
pub use file_id::FileId;

mod needle;
pub use needle::{Needle, NeedleValue};

mod needle_map;
pub use needle_map::{index_entry, walk_index_file, NeedleMapType, NeedleMapper};

mod needle_value_map;
pub use needle_value_map::{MemoryNeedleValueMap, NeedleValueMap};

mod replica_placement;
pub use replica_placement::ReplicaPlacement;

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
pub use volume::vacuum::{
    batch_vacuum_volume_check, batch_vacuum_volume_commit, batch_vacuum_volume_compact,
};

mod volume_info;
pub use volume_info::VolumeInfo;

pub const BUFFER_SIZE_LIMIT: usize = 2 * 1024 * 1024;
