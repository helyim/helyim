use std::{result::Result as StdResult, str::FromStr};

use axum::{
    body::Body,
    extract::{FromRequest, Query, State},
    http::{
        header::CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, Method, Request, Response,
        StatusCode, Uri,
    },
    middleware::Next,
    response::IntoResponse,
    Form, Json, RequestExt,
};
use axum_macros::FromRequest;
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use serde::de::DeserializeOwned;
use tokio::net::TcpStream;
use tracing::{error, info};

use crate::{
    directory::http::DirectoryState,
    topology::{TopologyError, TopologyRef},
    util::http::FormOrJson,
};

#[derive(Debug, FromRequest)]
pub struct GetOrHeadExtractor {
    pub uri: Uri,
    pub headers: HeaderMap,
}

#[async_trait::async_trait]
impl<T> FromRequest<DirectoryState> for FormOrJson<T>
where
    Json<T>: FromRequest<DirectoryState>,
    Form<T>: FromRequest<DirectoryState>,
    T: DeserializeOwned + 'static,
{
    type Rejection = axum::response::Response;

    async fn from_request(
        req: Request<Body>,
        _state: &DirectoryState,
    ) -> StdResult<Self, Self::Rejection> {
        match req.method() {
            &Method::GET | &Method::HEAD => {
                let Query(payload) = req
                    .extract::<Query<T>, _>()
                    .await
                    .map_err(|err| err.into_response())?;

                return Ok(Self(payload));
            }
            &Method::POST => {
                let content_type = req
                    .headers()
                    .get(CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok())
                    .ok_or_else(|| StatusCode::BAD_REQUEST.into_response())?;

                if content_type.starts_with("application/json") {
                    let Json(payload) = req
                        .extract::<Json<T>, _>()
                        .await
                        .map_err(|err| err.into_response())?;

                    return Ok(Self(payload));
                } else if content_type.starts_with("application/x-www-form-urlencoded") {
                    let Form(payload) = req
                        .extract::<Form<T>, _>()
                        .await
                        .map_err(|err| err.into_response())?;

                    return Ok(Self(payload));
                }
            }
            _ => {}
        }
        error!(
            "invalid http request: url: {}, method: {}, content type is only support `form` and \
             `json`",
            req.uri(),
            req.method()
        );
        Err(StatusCode::BAD_REQUEST.into_response())
    }
}

async fn proxy_to_leader(
    mut req: Request<Body>,
    topology: &TopologyRef,
) -> Result<Response<Incoming>, TopologyError> {
    return match topology.current_leader_address().await {
        Some(addr) => {
            let path = req.uri().path();
            let path_query = req
                .uri()
                .path_and_query()
                .map(|v| v.as_str())
                .unwrap_or(path);

            let uri = format!("http://{addr}{}", path_query);
            info!("This server is not the leader, will redirect to {uri}");

            match Uri::try_from(uri) {
                Ok(uri) => {
                    *req.uri_mut() = uri;

                    let stream = TcpStream::connect(&*addr).await?;
                    let io = TokioIo::new(stream);
                    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;
                    tokio::task::spawn(async move {
                        if let Err(err) = conn.await {
                            println!("Connection failed: {:?}", err);
                        }
                    });

                    let mut response = sender.send_request(req).await?;
                    response.headers_mut().insert(
                        HeaderName::from_str("HTTP_X_FORWARDED_FOR")?,
                        HeaderValue::from_str(&addr)?,
                    );
                    Ok(response)
                }
                Err(err) => Err(TopologyError::InvalidUrl(err)),
            }
        }
        None => Err(TopologyError::NoLeader),
    };
}

pub async fn require_leader(
    State(state): State<DirectoryState>,
    request: Request<Body>,
    next: Next,
) -> axum::response::Response {
    let topology = &state.topology;
    if !topology.is_leader().await {
        return proxy_to_leader(request, topology).await.into_response();
    }
    next.run(request).await
}
