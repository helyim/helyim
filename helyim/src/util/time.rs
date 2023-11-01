use std::time::{Duration, SystemTime, SystemTimeError};
pub fn now() -> Duration {
    let now = SystemTime::now();
    now.duration_since(SystemTime::UNIX_EPOCH).unwrap()
}

pub fn get_time(time: SystemTime) -> Result<Duration, SystemTimeError> {
    time.duration_since(SystemTime::UNIX_EPOCH)
}
