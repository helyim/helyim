use std::time::Duration;

use axum::{
    body::HttpBody,
    extract::{FromRequest, Query},
    http::{header::CONTENT_TYPE, StatusCode},
    response::{Html, IntoResponse},
    BoxError, Form, Json, RequestExt,
};
use bytes::Bytes;
use hyper::{body::to_bytes, header::CONTENT_LENGTH, Body, Client, Method, Request};
use serde::de::DeserializeOwned;
use tracing::error;
use url::Url;

use crate::{errors::Result, images::FAVICON_ICO, PHRASE};

pub async fn delete(url: &str, params: &[(&str, &str)]) -> Result<Bytes> {
    request(url, params, Method::DELETE, None).await
}

pub async fn get(url: &str, params: &[(&str, &str)]) -> Result<Bytes> {
    request(url, params, Method::GET, None).await
}

pub async fn post(url: &str, params: &[(&str, &str)], body: &[u8]) -> Result<Bytes> {
    request(url, params, Method::POST, Some(body)).await
}

async fn request(
    url: &str,
    params: &[(&str, &str)],
    method: Method,
    body: Option<&[u8]>,
) -> Result<Bytes> {
    let url = Url::parse_with_params(url, params)?;

    let request = Request::builder().method(method).uri(url.as_str());
    let request = match body {
        Some(body) => request
            .header(CONTENT_LENGTH, body.len())
            .body(Body::from(body.to_vec()))?,
        None => request.header(CONTENT_LENGTH, 0).body(Body::empty())?,
    };

    let client = Client::builder()
        .pool_idle_timeout(Duration::from_secs(30))
        .build_http();
    let mut response = client.request(request).await?;

    let body = to_bytes(response.body_mut()).await?;
    Ok(body)
}

pub struct FormOrJson<T>(pub T);

#[async_trait::async_trait]
impl<T, S, B> FromRequest<S, B> for FormOrJson<T>
where
    Json<T>: FromRequest<S, B>,
    Form<T>: FromRequest<S, B>,
    T: DeserializeOwned + 'static,
    B: HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<BoxError>,
    S: Send + Sync,
{
    type Rejection = axum::response::Response;

    async fn from_request(
        req: Request<B>,
        _state: &S,
    ) -> std::result::Result<Self, Self::Rejection> {
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
                } else {
                    error!(
                        "invalid http request: url: {}, method: {}, content type: {content_type}",
                        req.uri(),
                        req.method()
                    );
                }
            }
            _ => error!(
                "invalid http request: url: {}, method: {}",
                req.uri(),
                req.method()
            ),
        }

        Err(StatusCode::BAD_REQUEST.into_response())
    }
}

pub async fn default_handler() -> Html<&'static str> {
    Html(PHRASE)
}

pub async fn favicon_handler<'a>() -> Result<&'a [u8]> {
    FAVICON_ICO.bytes()
}
