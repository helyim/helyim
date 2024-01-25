use std::{result::Result as StdResult, str::FromStr, time::Duration};

use axum::{
    extract::{FromRequest, Query},
    headers::{HeaderMap, Host},
    http::{header::CONTENT_TYPE, HeaderName, HeaderValue, StatusCode},
    response::IntoResponse,
    Form, Json, RequestExt, TypedHeader,
};
use axum_macros::FromRequest;
use bytes::Bytes;
use faststr::FastStr;
use hyper::{client::HttpConnector, Body, Client, Method, Request, Response, Uri};
use once_cell::sync::Lazy;
use serde::{de::DeserializeOwned, Deserialize};
use tracing::{error, info};

use crate::{
    directory::DirectoryState,
    storage::VolumeId,
    topology::{TopologyError, TopologyRef},
};

pub(super) static HYPER_CLIENT: Lazy<Client<HttpConnector, Body>> = Lazy::new(|| {
    Client::builder()
        .pool_idle_timeout(Duration::from_secs(30))
        .build_http()
});

#[derive(Debug, FromRequest)]
pub struct GetOrHeadExtractor {
    pub uri: Uri,
    pub headers: HeaderMap,
}

#[derive(Debug, FromRequest)]
pub struct PostExtractor {
    // only the last field can implement `FromRequest`
    // other fields must only implement `FromRequestParts`
    pub uri: Uri,
    pub headers: HeaderMap,
    #[from_request(via(Query))]
    pub query: StorageQuery,
    pub body: Bytes,
}

#[derive(Debug, FromRequest)]
pub struct DeleteExtractor {
    // only the last field can implement `FromRequest`
    // other fields must only implement `FromRequestParts`
    pub uri: Uri,
    #[from_request(via(TypedHeader))]
    pub host: Host,
    #[from_request(via(Query))]
    pub query: StorageQuery,
}

#[derive(Debug, Deserialize)]
pub struct StorageQuery {
    pub r#type: Option<FastStr>,
    // is chunked file
    pub cm: Option<bool>,
    pub ttl: Option<FastStr>,
    // last modified
    pub ts: Option<u64>,
}

#[derive(Debug, FromRequest)]
pub struct ErasureCodingExtractor {
    #[from_request(via(Query))]
    pub query: ErasureCodingQuery,
}

#[derive(Debug, Deserialize)]
pub struct ErasureCodingQuery {
    pub volume: VolumeId,
    pub collection: Option<FastStr>,
}

pub struct FormOrJson<T>(pub T);

#[async_trait::async_trait]
impl<T> FromRequest<DirectoryState, Body> for FormOrJson<T>
where
    Json<T>: FromRequest<DirectoryState, Body>,
    Form<T>: FromRequest<DirectoryState, Body>,
    T: DeserializeOwned + 'static,
{
    type Rejection = axum::response::Response;

    async fn from_request(
        req: Request<Body>,
        state: &DirectoryState,
    ) -> StdResult<Self, Self::Rejection> {
        let topology = &state.topology;
        if !topology.is_leader().await {
            return Err(proxy_to_leader(req, topology).await.into_response());
        }

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
) -> Result<Response<Body>, TopologyError> {
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
                    let mut response = HYPER_CLIENT.request(req).await?;
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
