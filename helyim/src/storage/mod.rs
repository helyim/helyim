mod disk_location;

pub mod erasure_coding;

mod http;

mod needle;
pub use needle::{
    read_index_entry, walk_index_file, MemoryNeedleValueMap, Needle, NeedleError, NeedleMapType,
    NeedleMapper, NeedleValue, NeedleValueMap,
};

mod server;
pub use server::VolumeServer;

mod store;

mod version;
pub use version::CURRENT_VERSION;

mod volume;
pub use volume::{
    vacuum::{batch_vacuum_volume_check, batch_vacuum_volume_commit, batch_vacuum_volume_compact},
    ReplicaPlacement, VolumeError, VolumeInfo,
};

pub const BUFFER_SIZE_LIMIT: usize = 2 * 1024 * 1024;
