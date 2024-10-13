use std::time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH};

pub fn now() -> Duration {
    let now = SystemTime::now();
    now.duration_since(SystemTime::UNIX_EPOCH).unwrap()
}

pub fn get_time(time: SystemTime) -> Result<Duration, TimeError> {
    Ok(time.duration_since(SystemTime::UNIX_EPOCH)?)
}

pub fn timestamp_to_time(timestamp: u64) -> Result<SystemTime, TimeError> {
    let time = Duration::from_millis(timestamp);
    UNIX_EPOCH
        .checked_add(time)
        .ok_or(TimeError::InvalidTimestamp(timestamp))
}

#[derive(thiserror::Error, Debug)]
pub enum TimeError {
    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(u64),
    #[error("SystemTime error: {0}")]
    SystemTime(#[from] SystemTimeError),
}
