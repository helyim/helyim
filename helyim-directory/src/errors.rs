use helyim_topology::raft::types::RaftError;

#[derive(thiserror::Error, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum DirectoryError {
    #[error("{0}")]
    Box(Box<dyn std::error::Error>),
    #[error("Raft error: {0}")]
    Raft(#[from] RaftError),
}
