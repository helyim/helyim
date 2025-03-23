#![allow(dead_code)]

pub mod erasure_coding;
#[macro_use]
pub mod node;

pub mod collection;

mod data_center;

mod data_node;

pub use data_node::DataNodeRef;

mod rack;
pub mod raft;

mod topology;

pub use topology::{Topology, TopologyError, TopologyRef, tests, topology_vacuum_loop};

pub mod volume;
pub mod volume_grow;

pub mod volume_layout;
