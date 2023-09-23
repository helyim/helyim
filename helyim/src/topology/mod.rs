pub mod collection;

mod data_center;
pub use data_center::{data_center_loop, DataCenter, DataCenterEvent, DataCenterEventTx};

mod data_node;
pub use data_node::{data_node_loop, DataNode, DataNodeEvent, DataNodeEventTx};

mod rack;
pub use rack::{rack_loop, Rack, RackEvent, RackEventTx};

#[allow(clippy::module_inception)]
mod topology;
pub use topology::{topology_loop, topology_vacuum_loop, Topology, TopologyEvent, TopologyEventTx};

mod vacuum;
// TODO: remove this lint, maybe it's clippy's bug
#[allow(clippy::mutable_key_type)]
pub mod volume_grow;
pub mod volume_layout;
