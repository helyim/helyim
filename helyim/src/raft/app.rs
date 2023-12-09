use std::sync::Arc;

use crate::raft::{NodeId, Raft, Store};

// Representation of an application state. This struct can be shared around to share
// instances of raft, store and more.
pub struct App {
    pub id: NodeId,
    pub addr: String,
    pub raft: Raft,
    pub store: Arc<Store>,
    pub config: Arc<openraft::Config>,
}
