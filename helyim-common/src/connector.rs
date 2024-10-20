use std::{future::Future, io::Error, pin::Pin};

use hyper::Uri;
use pin_project_lite::pin_project;
use tokio::io::AsyncWrite;
use tower::Service;
use turmoil::net::TcpStream;

type Fut = Pin<Box<dyn Future<Output = Result<TurmoilConnection, Error>> + Send>>;

pub fn connector(
) -> impl Service<Uri, Response = TurmoilConnection, Error = Error, Future = Fut> + Clone {
    tower::service_fn(|uri: Uri| {
        Box::pin(async move {
            let conn = TcpStream::connect(uri.authority().unwrap().as_str()).await?;
            Ok::<_, Error>(TurmoilConnection { fut: conn })
        }) as Fut
    })
}

pin_project! {
    pub struct TurmoilConnection{
        #[pin]
        fut: TcpStream
    }
}

impl hyper::rt::Read for TurmoilConnection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        mut buf: hyper::rt::ReadBufCursor<'_>,
    ) -> std::task::Poll<Result<(), Error>> {
        let n = unsafe {
            let mut tbuf = tokio::io::ReadBuf::uninit(buf.as_mut());
            let result = tokio::io::AsyncRead::poll_read(self.project().fut, cx, &mut tbuf);
            match result {
                std::task::Poll::Ready(Ok(())) => tbuf.filled().len(),
                other => return other,
            }
        };

        unsafe {
            buf.advance(n);
        }
        std::task::Poll::Ready(Ok(()))
    }
}

impl hyper::rt::Write for TurmoilConnection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, Error>> {
        Pin::new(&mut self.fut).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Error>> {
        Pin::new(&mut self.fut).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Error>> {
        Pin::new(&mut self.fut).poll_shutdown(cx)
    }
}

impl hyper_util::client::legacy::connect::Connection for TurmoilConnection {
    fn connected(&self) -> hyper_util::client::legacy::connect::Connected {
        hyper_util::client::legacy::connect::Connected::new()
    }
}
