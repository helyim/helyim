use std::time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH};
pub fn now() -> Duration {
    let now = SystemTime::now();
    now.duration_since(SystemTime::UNIX_EPOCH).unwrap()
}

// fix me: will lose accuracy
pub fn get_time(time: SystemTime) -> Result<Duration, SystemTimeError> {
    time.duration_since(SystemTime::UNIX_EPOCH)
}

pub fn get_system_time(unix_timestamp: u64) -> SystemTime {
    let duration = Duration::from_secs(unix_timestamp);
    UNIX_EPOCH + duration
}
