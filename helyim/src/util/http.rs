use std::time::Duration;

use axum::{
    body::HttpBody,
    extract::FromRequest,
    headers::HeaderValue,
    http::{header::CONTENT_TYPE, StatusCode},
    response::IntoResponse,
    BoxError, Form, Json, RequestExt,
};
use bytes::{BufMut, Bytes, BytesMut};
use hyper::{body::to_bytes, header::CONTENT_LENGTH, Body, Client, Method, Request};
use mime_guess::mime;
use serde::{de::DeserializeOwned, Serialize};
use url::Url;

use crate::errors::Result;

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

            Ok(Self(payload))
        } else if content_type.starts_with("application/x-www-form-urlencoded") {
            let Form(payload) = req
                .extract::<Form<T>, _>()
                .await
                .map_err(|err| err.into_response())?;

            Ok(Self(payload))
        } else {
            Err(StatusCode::BAD_REQUEST.into_response())
        }
    }
}

impl<T> IntoResponse for FormOrJson<T>
where
    T: Serialize,
{
    fn into_response(self) -> axum::response::Response {
        let mut buf = BytesMut::with_capacity(128).writer();
        match serde_json::to_writer(&mut buf, &self.0) {
            Ok(()) => (
                [(
                    CONTENT_TYPE,
                    HeaderValue::from_static(mime::APPLICATION_JSON.as_ref()),
                )],
                buf.into_inner().freeze(),
            )
                .into_response(),
            Err(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(
                    CONTENT_TYPE,
                    HeaderValue::from_static(mime::TEXT_PLAIN_UTF_8.as_ref()),
                )],
                err.to_string(),
            )
                .into_response(),
        }
    }
}
