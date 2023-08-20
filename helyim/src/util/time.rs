use std::time::{Duration, SystemTime};

use crate::errors::Result;

pub fn now() -> Duration {
    let now = SystemTime::now();
    now.duration_since(SystemTime::UNIX_EPOCH).unwrap()
}

pub fn get_time(time: SystemTime) -> Result<Duration> {
    Ok(time.duration_since(SystemTime::UNIX_EPOCH)?)
}
