/// A shortcut to box an error.
#[macro_export]
macro_rules! anyhow {
    ($e:expr) => ({
        use std::error::Error;
        let e: Box<dyn Error + Sync + Send> = ($e).into();
        e.into()
    });
    ($f:tt, $($arg:expr),+) => ({
        anyhow!(format!($f, $($arg),+))
    });
}
