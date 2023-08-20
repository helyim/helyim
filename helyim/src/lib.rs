use std::time::Duration;

use axum::response::Html;
use errors::Result;
use futures::channel::oneshot;
use hyper::{Body, Request as HttpRequest, Response as HttpResponse};

pub mod directory;

mod errors;
mod operation;

pub mod storage;

mod sequence;
mod util;

type Request = HttpRequest<Body>;
type Response = HttpResponse<Body>;
type ApiCallback = Box<dyn FnOnce(Result<Response>) + Send>;

const PHRASE: &str = "<h1>Hello, World!</h1>";
const DEFAULT: &str = "default";
const STOP_INTERVAL: Duration = Duration::from_secs(2);

pub enum Message {
    Api { req: Request, cb: ApiCallback },
}

pub fn make_callback() -> (ApiCallback, oneshot::Receiver<Result<Response>>) {
    let (tx, rx) = oneshot::channel();
    let callback = move |resp| {
        tx.send(resp).unwrap();
    };
    (Box::new(callback), rx)
}

pub async fn default_handler() -> Html<&'static str> {
    Html(PHRASE)
}

#[cfg(not(all(target_os = "linux", feature = "iouring")))]
pub use tokio::spawn as rt_spawn;
#[cfg(all(target_os = "linux", feature = "iouring"))]
pub use tokio_uring::spawn as rt_spawn;
