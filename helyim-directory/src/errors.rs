use helyim_common::parser::ParseError;
use helyim_topology::raft::types::RaftError;

#[derive(thiserror::Error, Debug)]
pub enum DirectoryError {
    #[error("Raft error: {0}")]
    Raft(#[from] RaftError),
    #[error("Broadcast send: {0}")]
    BroadcastSend(#[from] async_broadcast::SendError<()>),
    #[error("Parse error: {0}")]
    Parse(#[from] ParseError),
}
