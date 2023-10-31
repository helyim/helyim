use rustix::process::{getpid, kill_process, Signal};

/// Exit by terminate signal
pub fn exit() {
    let _ = kill_process(getpid(), Signal::Term);
}
